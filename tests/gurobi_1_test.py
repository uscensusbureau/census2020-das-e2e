#!/usr/bin/python

# Test that Gurobi works

#
# Note that this is the test program from 
# http://www.gurobi.com/documentation/7.5/examples/diet_py.html
# Turned into a confirmance test to make sure that gurobi is working...

# Copyright 2017, Gurobi Optimization, Inc.

# Solve the classic diet model, showing how to add constraints
# to an existing model.

import os
import sys
sys.path.append( os.path.dirname(__file__ ) )

from gurobipy import *
from das_testpoints import log_testpoint

# Nutrition guidelines, based on
# USDA Dietary Guidelines for Americans, 2005
# http://www.health.gov/DietaryGuidelines/dga2005/

categories, minNutrition, maxNutrition = multidict({
  'calories': [1800, 2200],
  'protein':  [91, GRB.INFINITY],
  'fat':      [0, 65],
  'sodium':   [0, 1779] })

foods, cost = multidict({
  'hamburger': 2.49,
  'chicken':   2.89,
  'hot dog':   1.50,
  'fries':     1.89,
  'macaroni':  2.09,
  'pizza':     1.99,
  'salad':     2.49,
  'milk':      0.89,
  'ice cream': 1.59 })

# Nutrition values for the foods
nutritionValues = {
  ('hamburger', 'calories'): 410,
  ('hamburger', 'protein'):  24,
  ('hamburger', 'fat'):      26,
  ('hamburger', 'sodium'):   730,
  ('chicken',   'calories'): 420,
  ('chicken',   'protein'):  32,
  ('chicken',   'fat'):      10,
  ('chicken',   'sodium'):   1190,
  ('hot dog',   'calories'): 560,
  ('hot dog',   'protein'):  20,
  ('hot dog',   'fat'):      32,
  ('hot dog',   'sodium'):   1800,
  ('fries',     'calories'): 380,
  ('fries',     'protein'):  4,
  ('fries',     'fat'):      19,
  ('fries',     'sodium'):   270,
  ('macaroni',  'calories'): 320,
  ('macaroni',  'protein'):  12,
  ('macaroni',  'fat'):      10,
  ('macaroni',  'sodium'):   930,
  ('pizza',     'calories'): 320,
  ('pizza',     'protein'):  15,
  ('pizza',     'fat'):      12,
  ('pizza',     'sodium'):   820,
  ('salad',     'calories'): 320,
  ('salad',     'protein'):  31,
  ('salad',     'fat'):      12,
  ('salad',     'sodium'):   1230,
  ('milk',      'calories'): 100,
  ('milk',      'protein'):  8,
  ('milk',      'fat'):      2.5,
  ('milk',      'sodium'):   125,
  ('ice cream', 'calories'): 330,
  ('ice cream', 'protein'):  8,
  ('ice cream', 'fat'):      10,
  ('ice cream', 'sodium'):   180 }

def test_gurobi():

    # Get the model using our environment variables
    try:
        env = Env.OtherEnv("gurobi.log", os.environ.get('GRB_ISV_NAME',''), os.environ.get('GRB_APP_NAME',''), 0, "")
        log_testpoint('T01-001S')
    except GurobiError as err:
        log_testpoint('T01-001F')
        raise err
    m = Model("diet", env = env)

    # Create decision variables for the foods to buy
    buy = m.addVars(foods, name="buy")

    # You could use Python looping constructs and m.addVar() to create
    # these decision variables instead.  The following would be equivalent
    #
    # buy = {}
    # for f in foods:
    #   buy[f] = m.addVar(name=f)

    # The objective is to minimize the costs
    m.setObjective(buy.prod(cost), GRB.MINIMIZE)

    # Using looping constructs, the preceding statement would be:
    #
    # m.setObjective(sum(buy[f]*cost[f] for f in foods), GRB.MINIMIZE)

    # Nutrition constraints
    m.addConstrs(
        (quicksum(nutritionValues[f,c] * buy[f] for f in foods)
            == [minNutrition[c], maxNutrition[c]]
         for c in categories), "_")

    # Using looping constructs, the preceding statement would be:
    #
    # for c in categories:
    #  m.addRange(
    #     sum(nutritionValues[f,c] * buy[f] for f in foods), minNutrition[c], 
    #         maxNutrition[c], c)

    def printSolution():
        if m.status == GRB.Status.OPTIMAL:
            print('\nCost: %g' % m.objVal)
            print('\nBuy:')
            buyx = m.getAttr('x', buy)
            for f in foods:
                if buy[f].x > 0.0001:
                    print('%s %g' % (f, buyx[f]))
        else:
            print('No solution')

    # Solve
    m.optimize()
    printSolution()
    assert m.status == GRB.Status.OPTIMAL

    print('\nAdding constraint: at most 6 servings of dairy')
    m.addConstr(buy.sum(['milk','ice cream']) <= 6, "limit_dairy")

    # Solve
    m.optimize()
    printSolution()
    assert m.status == GRB.Status.INFEASIBLE

if __name__=="__main__":
    test_gurobi()
    exit(0)

