from azure.kusto.data.exceptions import OneApiError


def test_parse_one_api_error():
    result = OneApiError.from_dict(
        {
            "code": "LimitsExceeded",
            "message": "Request is invalid and cannot be executed.",
            "@type": "Kusto.Data.Exceptions.KustoServicePartialQueryFailureLimitsExceededException",
            "@message": "Query execution has exceeded the allowed limits (80DA0003): .",
            "@context": {
                "timestamp": "2018-12-10T15:10:48.8352222Z",
                "machineName": "RD0003FFBEDEB9",
                "processName": "Kusto.Azure.Svc",
                "processId": 4328,
                "threadId": 7284,
                "appDomainName": "RdRuntime",
                "clientRequestId": "KPC.execute;d3a43e37-0d7f-47a9-b6cd-a889b2aee3d3",
                "activityId": "a57ec272-8846-49e6-b458-460b841ed47d",
                "subActivityId": "a57ec272-8846-49e6-b458-460b841ed47d",
                "activityType": "PO-OWIN-CallContext",
                "parentActivityId": "a57ec272-8846-49e6-b458-460b841ed47d",
                "activityStack": "(Activity stack: CRID=KPC.execute;d3a43e37-0d7f-47a9-b6cd-a889b2aee3d3 ARID=a57ec272-8846-49e6-b458-460b841ed47d > PO-OWIN-CallContext/a57ec272-8846-49e6-b458-460b841ed47d)",
            },
            "@permanent": False,
        }
    )

    assert result.code == "LimitsExceeded"
    assert result.type == "Kusto.Data.Exceptions.KustoServicePartialQueryFailureLimitsExceededException"
    assert result.message == "Request is invalid and cannot be executed."
    assert result.description == "Query execution has exceeded the allowed limits (80DA0003): ."
    assert not result.permanent
    assert result.context["timestamp"] == "2018-12-10T15:10:48.8352222Z"
    assert result.context["machineName"] == "RD0003FFBEDEB9"
    assert result.context["processName"] == "Kusto.Azure.Svc"
    assert result.context["processId"] == 4328
    assert result.context["threadId"] == 7284
    assert result.context["appDomainName"] == "RdRuntime"
    assert result.context["clientRequestId"] == "KPC.execute;d3a43e37-0d7f-47a9-b6cd-a889b2aee3d3"
    assert result.context["activityId"] == "a57ec272-8846-49e6-b458-460b841ed47d"
    assert result.context["subActivityId"] == "a57ec272-8846-49e6-b458-460b841ed47d"
    assert result.context["activityType"] == "PO-OWIN-CallContext"
    assert result.context["parentActivityId"] == "a57ec272-8846-49e6-b458-460b841ed47d"
    assert (
        result.context["activityStack"] == "(Activity stack: CRID=KPC.execute;d3a43e37-0d7f-47a9-b6cd-a889b2aee3d3 ARID=a57ec272-8846-49e6-b458-460b841ed47d "
        "> PO-OWIN-CallContext/a57ec272-8846-49e6-b458-460b841ed47d)"
    )
