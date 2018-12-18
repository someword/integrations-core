# (C) Datadog, Inc. 2018
# All rights reserved
# Licensed under a 3-clause BSD style license (see LICENSE)

import json
from datetime import datetime, timedelta

from six import iteritems, string_types
from kubernetes.config.dateutil import format_rfc3339

from datadog_checks.base.checks.kube_leader import ElectionRecord


def make_record(holder=None, duration=None, transitions=None, acquire=None, renew=None):
    def format_time(date_time):
        if isinstance(date_time, string_types):
            return date_time
        return format_rfc3339(date_time)

    record = {}
    if holder:
        record["holderIdentity"] = holder
    if duration:
        record["leaseDurationSeconds"] = duration
    if transitions:
        record["leaderTransitions"] = transitions
    if acquire:
        record["acquireTime"] = format_time(acquire)
    if renew:
        record["renewTime"] = format_time(renew)

    return json.dumps(record)


class TestElectionRecord:
    def test_parse_raw(self):
        raw = ('{"holderIdentity":"dd-cluster-agent-568f458dd6-kj6vt",'
               '"leaseDurationSeconds":60,'
               '"acquireTime":"2018-12-17T11:53:07Z",'
               '"renewTime":"2018-12-18T12:32:22Z",'
               '"leaderTransitions":7}')
        record = ElectionRecord(raw)

        valid, reason = record.validate()
        assert valid is True
        assert reason is None

        assert record.leader_name == "dd-cluster-agent-568f458dd6-kj6vt"
        assert record.lease_duration == 60
        assert record.transitions == 7
        assert record.renew_time > record.acquire_time
        assert record.seconds_until_renew < 0
        assert "{}".format(record) == ("Leader: dd-cluster-agent-568f458dd6-kj6vt "
                                       "since 2018-12-17 11:53:07+00:00, "
                                       "next renew 2018-12-18 12:32:22+00:00")

    def test_validation(self):
        cases = {
            make_record(): "Invalid record: no current leader recorded",
            make_record(holder="me"): "Invalid record: no lease duration set",
            make_record(holder="me", duration=30): "Invalid record: no renew time set",
            make_record(
                holder="me", duration=30, renew="2018-12-18T12:32:22Z"
            ): "Invalid record: no acquire time recorded",
            make_record(holder="me", duration=30, renew=datetime.now(), acquire="2018-12-18T12:32:22Z"): None,
            make_record(
                holder="me", duration=30, renew="invalid", acquire="2018-12-18T12:32:22Z"
            ): "Invalid record: bad format for renewTime field",
            make_record(
                holder="me", duration=30, renew="2018-12-18T12:32:22Z", acquire="0000-12-18T12:32:22Z"
            ): "Invalid record: bad format for acquireTime field",
        }

        for raw, expected_reason in iteritems(cases):
            valid, reason = ElectionRecord(raw).validate()
            assert reason == expected_reason
            if expected_reason is None:
                assert valid is True
            else:
                assert valid is False

    def test_seconds_until_renew(self):
        raw = make_record(
            holder="me", duration=30, acquire="2018-12-18T12:32:22Z", renew=datetime.now() + timedelta(seconds=20)
        )

        record = ElectionRecord(raw)
        assert record.seconds_until_renew > 19
        assert record.seconds_until_renew < 21

        raw = make_record(
            holder="me", duration=30, acquire="2018-12-18T12:32:22Z", renew=datetime.now() - timedelta(seconds=5)
        )

        record = ElectionRecord(raw)
        assert record.seconds_until_renew > -6
        assert record.seconds_until_renew < -4


class TestMixin:
    pass
