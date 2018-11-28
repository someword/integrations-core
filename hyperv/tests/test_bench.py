# (C) Datadog, Inc. 2018
# All rights reserved
# Licensed under a 3-clause BSD style license (see LICENSE)
from copy import deepcopy

from datadog_checks.hyperv import HypervCheck


def test_cache(benchmark, instance):
    instance = deepcopy(instance)
    instance['cache_counter_instances'] = True
    check = HypervCheck('hyperv', {}, {}, [instance])

    # Run once to get any PDH setup out of the way.
    check.check(instance)

    benchmark(check.check, instance)


def test_no_cache(benchmark, instance):
    instance = deepcopy(instance)
    instance['cache_counter_instances'] = False
    check = HypervCheck('hyperv', {}, {}, [instance])

    # Run once to get any PDH setup out of the way.
    check.check(instance)

    benchmark(check.check, instance)
