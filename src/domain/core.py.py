#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# =========================================================================== #
# Project  : Drug Approval Analytics                                          #
# Version  : 0.1.0                                                            #
# File     : \src\platform\database\base.py                                   #
# Language : Python 3.9.5                                                     #
# --------------------------------------------------------------------------  #
# Author   : John James                                                       #
# Company  : nov8.ai                                                          #
# Email    : john.james@nov8.ai                                               #
# URL      : https://github.com/john-james-sf/drug-approval-analytics         #
# --------------------------------------------------------------------------  #
# Created  : Monday, August 9th 2021, 11:44:10 pm                             #
# Modified : Sunday, August 15th 2021, 2:34:04 am                             #
# Modifier : John James (john.james@nov8.ai)                                  #
# --------------------------------------------------------------------------- #
# License  : BSD 3-clause "New" or "Revised" License                          #
# Copyright: (c) 2021 nov8.ai                                                 #
# =========================================================================== #
from abc import ABC, abstractmethod

# --------------------------------------------------------------------------- #
#                               PIPELINE                                      #
# --------------------------------------------------------------------------- #


class Operator(ABC):
    """Abstract base class for pipeline or worfklow tasks.

    Follows the basic Apache Airflow API to support future Airflow
    integration with Airflow.

    Reference:
        https://airflow.apache.org/docs/apache-airflow/
            1.10.12/_api/airflow/models/index.html#airflow.
            models.BaseOperator

    """

    def __init__(task_id, owner="rx2m", email=None, email_on_retry=True,
                 email_on_failure=True, retries=0,
                 retry_delay=timedelta(seconds=300),
                 retry_exponential_backoff=False, max_retry_delay=None,
                 start_date=None, end_date=None, schedule_interval=None,
                 depends_on_past=False, wait_for_downstream=False,
                 dag=None, params=None, default_args=None,
                 priority_weight=1, weight_rule=None, queue=None,
                 pool=None, pool_slots=1, sla=None,
                 execution_timeout=None, on_failure_callback=None,
                 on_success_callback=None, on_retry_callback=None,
                 trigger_rule=None, resources=None, run_as_user=None,
                 task_concurrency=None, executor_config=None,
                 do_xcom_push=True, inlets=None, outlets=None,
                 *args, **kwargs):
        """super(Operator, self).__init__(task_id, owner, email,
                                   email_on_retry, email_on_failure,
                                   retries, 'default_task_retries',
                                   fallback, retry_delay,
                                   retry_exponential_backoff,
                                   max_retry_delay,
                                   start_date, end_date,
                                   schedule_interval, depends_on_past,
                                   wait_for_downstream, dag,
                                   params, default_args, priority_weight,
                                   weight_rule, queue, pool, pool_slots,
                                   sla, execution_timeout,
                                   on_failure_callback,
                                   on_success_callback,
                                   on_retry_callback,
                                   trigger_rule, resources,
                                   run_as_user, task_concurrency,
                                   executor_config,
                                   do_xcom_push,
                                   inlets, outlets
                                   ) """
        pass

    @abstractmwthod
    def execute(self, context: dict) -> Any:
        pass
