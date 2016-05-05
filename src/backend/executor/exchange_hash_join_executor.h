//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// hash_join_executor.h
//
// Identification: src/backend/executor/hash_join_executor.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <deque>
#include <vector>

#include "backend/executor/abstract_join_executor.h"
//#include "backend/planner/exchange_hash_join_plan.h"
#include "backend/planner/hash_join_plan.h"
//#include "backend/executor/hash_executor.h"
#include "backend/executor/exchange_hash_executor.h"
//#include "backend/executor/abstract_parallel_executor.h"
#include "backend/executor/abstract_exchange_executor.h"

#include "backend/common/thread_manager.h"
#include "boost/lockfree/queue.hpp"
#include <atomic>

namespace peloton {
namespace executor {

class ExchangeHashJoinExecutor : public AbstractJoinExecutor, public AbstractExchangeExecutor {
      ExchangeHashJoinExecutor(const ExchangeHashJoinExecutor &) = delete;
      ExchangeHashJoinExecutor &operator=(const ExchangeHashJoinExecutor &) = delete;

      public:
      explicit ExchangeHashJoinExecutor(const planner::AbstractPlan *node,
                                        ExecutorContext *executor_context);

 protected:
  bool DInit();

  bool DExecute();

 private:
//  HashExecutor *hash_executor_ = nullptr;
  ExchangeHashExecutor *hash_executor_ = nullptr;

  bool hashed_ = false;

  std::deque<LogicalTile *> buffered_output_tiles;
  std::vector<std::unique_ptr<LogicalTile>> right_tiles_;

  // logical tile iterators
  size_t left_logical_tile_itr_ = 0;
  size_t right_logical_tile_itr_ = 0;
};

}  // namespace executor
}  // namespace peloton
