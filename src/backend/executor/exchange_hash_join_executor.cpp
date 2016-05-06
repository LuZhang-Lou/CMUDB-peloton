//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// hash_join_executor.cpp
//
// Identification: src/backend/executor/hash_join_executor.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <vector>

#include "backend/common/types.h"
#include "backend/common/logger.h"
#include "backend/executor/logical_tile_factory.h"
#include "backend/executor/exchange_hash_join_executor.h"
#include "backend/expression/abstract_expression.h"
#include "backend/expression/container_tuple.h"

namespace peloton {
namespace executor {

/**
 * @brief Constructor for hash join executor.
 * @param node Hash join node corresponding to this executor.
 */
ExchangeHashJoinExecutor::ExchangeHashJoinExecutor(const planner::AbstractPlan *node,
                                   ExecutorContext *executor_context)
    : AbstractJoinExecutor(node, executor_context) {}

bool ExchangeHashJoinExecutor::DInit() {
  printf("hello. ExchangeHashJOinExecutor\n");

  assert(children_.size() == 2);

  auto status = AbstractJoinExecutor::DInit();
  if (status == false) return status;

  assert(children_[1]->GetRawNode()->GetPlanNodeType() == PLAN_NODE_TYPE_HASH);

//  hash_executor_ = reinterpret_cast<HashExecutor *>(children_[1]);
  hash_executor_ = reinterpret_cast<ExchangeHashExecutor *>(children_[1]);
  left_tile_cnt = 0;
  left_tile_cnt_done = 0;

  return true;
}


void ExchangeHashJoinExecutor::Probe(size_t left_tile_idx){
//  printf("pick..up..%lu\n", left_tile_idx);
  LogicalTile *left_tile = left_result_tiles_[left_tile_idx].get();
  //===------------------------------------------------------------------===//
  // Build Join Tile
  //===------------------------------------------------------------------===//

  // Get the hash table from the hash executor
  auto &hash_table = hash_executor_->GetHashTable();
  auto &hashed_col_ids = hash_executor_->GetHashKeyIds();

  oid_t prev_tile = INVALID_OID;
  std::unique_ptr<LogicalTile> output_tile;
  LogicalTile::PositionListsBuilder pos_lists_builder;

  // Go over the left tile
  for (auto left_tile_itr : *left_tile) {
    const expression::ContainerTuple<executor::LogicalTile> left_tuple(
        left_tile, left_tile_itr, &hashed_col_ids);


    executor::ExchangeHashExecutor::MapValueType right_tuples;
    bool if_match = hash_table.find(left_tuple, right_tuples);

    if (if_match){

      RecordMatchedLeftRow(left_tile_idx, left_tile_itr);

      // Go over the matching right tuples
      for (auto &location : right_tuples) {
        // Check if we got a new right tile itr
        if (prev_tile != location.first) {
          // Check if we have any join tuples
          if (pos_lists_builder.Size() > 0) {
            LOG_TRACE("Join tile size : %lu \n", pos_lists_builder.Size());
            output_tile->SetPositionListsAndVisibility(
                pos_lists_builder.Release());
            lockfree_buffered_output_tiles.push(output_tile.release());

          }

          // Get the logical tile from right child
          LogicalTile *right_tile = right_result_tiles_[location.first].get();

          // Build output logical tile
          output_tile = BuildOutputLogicalTile(left_tile, right_tile);

          // Build position lists
          pos_lists_builder =
              LogicalTile::PositionListsBuilder(left_tile, right_tile);

          pos_lists_builder.SetRightSource(
              &right_result_tiles_[location.first]->GetPositionLists());
        }

        // Add join tuple
        pos_lists_builder.AddRow(left_tile_itr, location.second);

        RecordMatchedRightRow(location.first, location.second);

        // Cache prev logical tile itr
        prev_tile = location.first;
      }
    }
  }// end of go over the left tile ..

  // Check if we have any join tuples
  if (pos_lists_builder.Size() > 0) {
    LOG_TRACE("Join tile size : %lu \n", pos_lists_builder.Size());
    output_tile->SetPositionListsAndVisibility(pos_lists_builder.Release());
    lockfree_buffered_output_tiles.push(output_tile.release());
  }
//  int tmp = left_tile_cnt_done++;
  left_tile_cnt_done++;

//  printf("left_tile_cnt_done:%d\n", tmp);
}

/**
 * @brief Creates logical tiles from the two input logical tiles after applying
 * join predicate.
 * @return true on success, false otherwise.
 */
bool ExchangeHashJoinExecutor::DExecute() {
//  printf("********** Exchange Hash Join executor :: 2 children \n");

  for (;;) {



    // Check if we have any buffered output tiles
    if (lockfree_buffered_output_tiles.empty() == false) {
      LogicalTile *output_tile = nullptr;
      lockfree_buffered_output_tiles.pop(output_tile);
      SetOutput(output_tile);
      return true;
    } else if (left_child_done_){
//      int left_tile_cnt_tmp = left_tile_cnt;
//      int left_tile_cnt_done_tmp = left_tile_cnt_done;
//      printf("left_tile_cnt_tmp : %d, left_tile_cnt_done_tmp : %d\n", left_tile_cnt_tmp, left_tile_cnt_done_tmp);
      assert(left_tile_cnt >= left_tile_cnt_done);
      if ((!left_child_done_) || (left_child_done_ && left_tile_cnt > left_tile_cnt_done )){
          continue;
//        continue;
//        sleep(5);
//        return false;
      }else if (left_child_done_ && left_tile_cnt == left_tile_cnt_done){
        return BuildOuterJoinOutput();
      }
    }


    //===------------------------------------------------------------------===//
    // Pick right and left tiles
    //===---------------------------------------------------------------===//

    // Get all the tiles from RIGHT child
    if (right_child_done_ == false) {
      const auto start_build = std::chrono::system_clock::now();
      while (children_[1]->Execute()) {
        BufferRightTile(children_[1]->GetOutput());
      }
      right_child_done_ = true;
      printf("hash_table's size:%lu\n", hash_executor_->GetHashTable().size());

      const auto end_build = std::chrono::system_clock::now();
      const std::chrono::duration<double> diff = end_build-start_build;
      const double ms = diff.count()*1000;
      printf("ExchangeHash takes %lf ms\n", ms);
    }

    // Get next tile from LEFT child
    if (children_[0]->Execute() == false) {
      printf("!!!!!!!!!Did not get left tile \n");
      left_child_done_ = true;
      continue;
    }

    BufferLeftTile(children_[0]->GetOutput());
//    printf("Got left tile \n");

    if (right_result_tiles_.size() == 0) {
      printf("Did not get any right tiles \n");
      return BuildOuterJoinOutput();
    }

    size_t left_tile_idx = left_tile_cnt++;

    std::function<void()> probe_worker =
                std::bind(&ExchangeHashJoinExecutor::Probe, this, left_tile_idx);
    LaunchWorkerThreads( probe_worker);


    // Check if we have any buffered output tiles
    if (lockfree_buffered_output_tiles.empty() == false) {
      LogicalTile *output_tile = nullptr;
      lockfree_buffered_output_tiles.pop(output_tile);
      SetOutput(output_tile);
      return true;
    } else {
      assert(left_tile_cnt >= left_tile_cnt_done);
//      int left_tile_cnt_tmp = left_tile_cnt;
//      int left_tile_cnt_done_tmp = left_tile_cnt_done;
//      printf("left_tile_cnt_tmp : %d, left_tile_cnt_done_tmp : %d\n", left_tile_cnt_tmp, left_tile_cnt_done_tmp);
      if ((!left_child_done_) || (left_child_done_ && left_tile_cnt > left_tile_cnt_done )){
//        continue;
//        sleep(5);
//        return false;
      }else if (left_child_done_ && left_tile_cnt == left_tile_cnt_done){
        return BuildOuterJoinOutput();
      }
    }
  }
}

}  // namespace executor
}  // namespace peloton
