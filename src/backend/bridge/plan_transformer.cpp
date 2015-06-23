/*
 * transformer.cpp
 *
 *  Copyright(c) 2015, CMU
 *  Created on: Jun 18, 2015
 *      Author: Ming Fang
 */
extern "C" {
#include "nodes/pprint.h"
}
#include "backend/bridge/plan_transformer.h"
#include "backend/storage/data_table.h"
#include "backend/planner/insert_node.h"

extern "C" void printPlanStateTree(const PlanState * planstate);

namespace nstore {
namespace planner {

PlanTransformer&
PlanTransformer::GetInstance() {
  static PlanTransformer planTransformer;
  return planTransformer;
}

void PlanTransformer::printPostgresPlanStateTree(
    const PlanState * planstate) const {
  printPlanStateTree(planstate);
}

/* @brief Convert Postgres PlanState  into Peloton AbstractPlanNode.
 *
 *
 */
PlanTransformer::AbstractPlanNodePtr PlanTransformer::transform(
    const PlanState * planstate) {
  Plan *plan = planstate->plan;
  AbstractPlanNodePtr planNode;

  /* 1. Plan Type */
  switch (nodeTag(plan)) {
    case T_ModifyTable:
      planNode = transformModifyTable((ModifyTableState *) planstate);
      break;
    default:
      break;
  }

  return planNode;
}

PlanTransformer::AbstractPlanNodePtr transformModifyTable(
    ModifyTableState* planstate) {
  /*TODO: Actually implement this function */
  storage::DataTable *rtable = nullptr;
  std::vector<storage::Tuple *> tuples;
  return PlanTransformer::AbstractPlanNodePtr(new planner::InsertNode(rtable, tuples));
}

}
}
