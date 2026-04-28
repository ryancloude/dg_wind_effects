from __future__ import annotations

from typing import Final

from aws_cdk import RemovalPolicy
from aws_cdk import Stack
from aws_cdk import aws_dynamodb as dynamodb
from aws_cdk import aws_ec2 as ec2
from aws_cdk import aws_ecr as ecr
from aws_cdk import aws_ecs as ecs
from aws_cdk import aws_ssm as ssm
from constructs import Construct

from dgwe_cdk.config.jobs import JOB_DEFINITIONS
from dgwe_cdk.config.settings import PipelineSettings


CONFIG_PARAMETER_VALUES: Final[dict[str, str]] = {
    "PDGA_S3_BUCKET": "",
    "PDGA_DDB_TABLE": "",
    "PDGA_DDB_STATUS_END_DATE_GSI": "",
    "AWS_REGION": "",
    "ATHENA_DATABASE": "",
    "ATHENA_WORKGROUP": "",
    "ATHENA_RESULTS_S3_URI": "",
    "ATHENA_SOURCE_SCORED_TABLE": "",
    "ATHENA_REPORTING_BASE_TABLE": "",
    "PRODUCTION_TRAINING_REQUEST_FINGERPRINT": "",
}


class PipelineSharedStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        settings: PipelineSettings,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.settings = settings

        self.vpc = ec2.Vpc(
            self,
            "Vpc",
            ip_addresses=ec2.IpAddresses.cidr("10.42.0.0/16"),
            max_azs=2,
            nat_gateways=0,
            subnet_configuration=[
                ec2.SubnetConfiguration(
                    name="public",
                    subnet_type=ec2.SubnetType.PUBLIC,
                    cidr_mask=24,
                )
            ],
        )

        self.cluster = ecs.Cluster(
            self,
            "Cluster",
            cluster_name=settings.cluster_name,
            vpc=self.vpc,
            container_insights=True,
        )

        self.pipeline_runs_table = dynamodb.Table(
            self,
            "PipelineRunsTable",
            table_name=settings.pipeline_runs_table_name,
            partition_key=dynamodb.Attribute(
                name="run_id",
                type=dynamodb.AttributeType.STRING,
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.RETAIN,
            point_in_time_recovery=True,
        )

        self.config_parameters = self._create_config_parameters(settings)
        self.job_repositories = self._create_job_repositories()

    def _create_config_parameters(
        self,
        settings: PipelineSettings,
    ) -> dict[str, ssm.StringParameter]:
        parameter_values = {
            "PDGA_S3_BUCKET": settings.pdga_s3_bucket,
            "PDGA_DDB_TABLE": settings.pdga_ddb_table,
            "PDGA_DDB_STATUS_END_DATE_GSI": settings.pdga_ddb_status_end_date_gsi,
            "AWS_REGION": settings.aws_region,
            "ATHENA_DATABASE": settings.athena_database,
            "ATHENA_WORKGROUP": settings.athena_workgroup,
            "ATHENA_RESULTS_S3_URI": settings.athena_results_s3_uri,
            "ATHENA_SOURCE_SCORED_TABLE": settings.athena_source_scored_table,
            "ATHENA_REPORTING_BASE_TABLE": settings.athena_reporting_base_table,
            "PRODUCTION_TRAINING_REQUEST_FINGERPRINT": settings.production_training_request_fingerprint,
        }

        parameters: dict[str, ssm.StringParameter] = {}
        for env_key, value in parameter_values.items():
            parameters[env_key] = ssm.StringParameter(
                self,
                f"{env_key}Parameter",
                parameter_name=settings.parameter_name(env_key),
                string_value=value,
            )
        return parameters

    def _create_job_repositories(self) -> dict[str, ecr.Repository]:
        repositories: dict[str, ecr.Repository] = {}
        for definition in JOB_DEFINITIONS:
            repositories[definition.job_name] = ecr.Repository(
                self,
                f"{definition.state_id}Repository",
                repository_name=definition.ecr_repo_name,
                image_scan_on_push=True,
                removal_policy=RemovalPolicy.RETAIN,
                empty_on_delete=False,
            )
        return repositories
