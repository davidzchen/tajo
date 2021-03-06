/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tajo.plan.rewrite;

import org.apache.tajo.OverridableConf;
import org.apache.tajo.catalog.CatalogService;
import org.apache.tajo.plan.LogicalPlan;

public class LogicalPlanRewriteRuleContext {

  private OverridableConf queryContext;
  private LogicalPlan plan;
  private CatalogService catalog;

  public LogicalPlanRewriteRuleContext(OverridableConf queryContext, LogicalPlan plan) {
    setQueryContext(queryContext);
    setPlan(plan);
  }

  public LogicalPlanRewriteRuleContext(OverridableConf queryContext, LogicalPlan plan, CatalogService catalog) {
    setQueryContext(queryContext);
    setPlan(plan);
    setCatalog(catalog);
  }

  public void setCatalog(CatalogService catalog) {
    this.catalog = catalog;
  }

  public CatalogService getCatalog() {
    return catalog;
  }

  public OverridableConf getQueryContext() {
    return queryContext;
  }

  public void setQueryContext(OverridableConf queryContext) {
    this.queryContext = queryContext;
  }

  public LogicalPlan getPlan() {
    return plan;
  }

  public void setPlan(LogicalPlan plan) {
    this.plan = plan;
  }
}
