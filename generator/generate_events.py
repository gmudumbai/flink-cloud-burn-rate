#!/usr/bin/env python3
"""
Streams CloudTrail-shaped EC2 lifecycle events (RunInstances / TerminateInstances)
into a Kafka/Redpanda topic. A fraction of events are deliberately backdated to
simulate the out-of-order and late arrival that real CloudTrail delivery exhibits.

Field names for the CloudTrail-standard parts of the event (eventTime, eventName,
awsRegion, recipientAccountId, userIdentity.arn, requestParameters/responseElements
instance shapes) follow the real CloudTrail EC2 log format documented at:
https://docs.aws.amazon.com/awscloudtrail/latest/userguide/cloudtrail-log-file-examples.html
"""
import argparse
import json
import random
import time
import uuid
from datetime import datetime, timedelta, timezone

from kafka import KafkaProducer

# Matches the instance types that pricing/ec2-us-east-1.json (Phase 2) will price.
INSTANCE_TYPES = [
    "t3.micro", "t3.medium", "m5.large", "m5.xlarge", "m5.2xlarge",
    "c5.large", "c5.xlarge", "c5.2xlarge", "r5.large", "r5.xlarge",
]

TEAMS = ["platform", "ml", "web"]
REGION = "us-east-1"
USER_NAMES = ["alice", "bob", "carla", "deepak", "erin"]


def make_account_ids(n):
    return [f"{100000000000 + i}" for i in range(n)]


def iso(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def new_instance_id():
    return "i-" + uuid.uuid4().hex[:17]


def run_instances_event(now, account_id, instance_id, instance_type, team):
    user = random.choice(USER_NAMES)
    return {
        "eventVersion": "1.08",
        "userIdentity": {
            "type": "IAMUser",
            "principalId": uuid.uuid4().hex[:21].upper(),
            "arn": f"arn:aws:iam::{account_id}:user/{user}",
            "accountId": account_id,
            "accessKeyId": "AKIA" + uuid.uuid4().hex[:16].upper(),
            "userName": user,
        },
        "eventTime": iso(now),
        "eventSource": "ec2.amazonaws.com",
        "eventName": "RunInstances",
        "awsRegion": REGION,
        "sourceIPAddress": "203.0.113.%d" % random.randint(1, 254),
        "userAgent": "aws-cli/2.15.0",
        "requestParameters": {
            "instancesSet": {"items": [{"instanceType": instance_type, "minCount": 1, "maxCount": 1}]},
            "tagSpecificationSet": {
                "items": [{"resourceType": "instance", "tags": [{"key": "team", "value": team}]}]
            },
        },
        "responseElements": {
            "requestId": str(uuid.uuid4()),
            "instancesSet": {
                "items": [
                    {
                        "instanceId": instance_id,
                        "instanceType": instance_type,
                        "currentState": {"code": 0, "name": "pending"},
                        "previousState": {"code": 0, "name": "pending"},
                    }
                ]
            },
        },
        "requestID": str(uuid.uuid4()),
        "eventID": str(uuid.uuid4()),
        "readOnly": False,
        "eventType": "AwsApiCall",
        "managementEvent": True,
        "recipientAccountId": account_id,
        "eventCategory": "Management",
        "tags": {"team": team},
    }


def terminate_instances_event(now, account_id, instance_id, instance_type, team):
    user = random.choice(USER_NAMES)
    return {
        "eventVersion": "1.08",
        "userIdentity": {
            "type": "IAMUser",
            "principalId": uuid.uuid4().hex[:21].upper(),
            "arn": f"arn:aws:iam::{account_id}:user/{user}",
            "accountId": account_id,
            "accessKeyId": "AKIA" + uuid.uuid4().hex[:16].upper(),
            "userName": user,
        },
        "eventTime": iso(now),
        "eventSource": "ec2.amazonaws.com",
        "eventName": "TerminateInstances",
        "awsRegion": REGION,
        "sourceIPAddress": "203.0.113.%d" % random.randint(1, 254),
        "userAgent": "aws-cli/2.15.0",
        "requestParameters": {
            "instancesSet": {"items": [{"instanceId": instance_id}]}
        },
        "responseElements": {
            "requestId": str(uuid.uuid4()),
            "instancesSet": {
                "items": [
                    {
                        "instanceId": instance_id,
                        "instanceType": instance_type,
                        "currentState": {"code": 32, "name": "shutting-down"},
                        "previousState": {"code": 16, "name": "running"},
                    }
                ]
            },
        },
        "requestID": str(uuid.uuid4()),
        "eventID": str(uuid.uuid4()),
        "readOnly": False,
        "eventType": "AwsApiCall",
        "managementEvent": True,
        "recipientAccountId": account_id,
        "eventCategory": "Management",
        "tags": {"team": team},
    }


def backdate(now):
    """~10% of events are 5-20s late, ~2% are 60-179s late. Returns (eventTime, label)."""
    roll = random.random()
    if roll < 0.02:
        delay = random.uniform(60, 179)
        return now - timedelta(seconds=delay), f"LATE(+{delay:.0f}s)"
    if roll < 0.12:
        delay = random.uniform(5, 20)
        return now - timedelta(seconds=delay), f"late(+{delay:.0f}s)"
    return now, "on-time"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rate", type=float, default=5.0, help="events per second (default 5)")
    parser.add_argument("--accounts", type=int, default=3, help="number of AWS accounts to simulate (default 3)")
    parser.add_argument("--duration", type=int, default=60, help="seconds to run (default 60)")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--topic", default="cloudtrail-events")
    args = parser.parse_args()

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    account_ids = make_account_ids(args.accounts)
    # accountId -> list of {instanceId, instanceType, team}
    running = {acc: [] for acc in account_ids}

    interval = 1.0 / args.rate
    end_time = time.monotonic() + args.duration
    sent = 0

    while time.monotonic() < end_time:
        now = datetime.now(timezone.utc)
        account_id = random.choice(account_ids)
        instances = running[account_id]

        # Terminate an existing instance about half the time once there's something to stop.
        if instances and random.random() < 0.5:
            idx = random.randrange(len(instances))
            inst = instances.pop(idx)
            event_time, label = backdate(now)
            event = terminate_instances_event(
                event_time, account_id, inst["instanceId"], inst["instanceType"], inst["team"]
            )
            print(f"[{label:14}] TerminateInstances account={account_id} "
                  f"instance={inst['instanceId']} type={inst['instanceType']} team={inst['team']}")
        else:
            instance_type = random.choice(INSTANCE_TYPES)
            team = random.choice(TEAMS)
            instance_id = new_instance_id()
            running[account_id].append(
                {"instanceId": instance_id, "instanceType": instance_type, "team": team}
            )
            event_time, label = backdate(now)
            event = run_instances_event(event_time, account_id, instance_id, instance_type, team)
            print(f"[{label:14}] RunInstances      account={account_id} "
                  f"instance={instance_id} type={instance_type} team={team}")

        producer.send(args.topic, value=event)
        sent += 1
        time.sleep(interval)

    producer.flush()
    print(f"\nDone. Sent {sent} events to topic '{args.topic}'.")


if __name__ == "__main__":
    main()
