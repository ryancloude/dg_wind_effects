from __future__ import annotations

import os
from pathlib import Path

import aws_cdk as cdk

from dgwe_cdk.config.settings import PipelineSettings
from dgwe_cdk.stacks.orchestration_stack import PipelineOrchestrationStack
from dgwe_cdk.stacks.shared_stack import PipelineSharedStack


def main() -> None:
    app = cdk.App()

    repo_root = Path(__file__).resolve().parents[1]
    env_file = repo_root / ".env"

    settings = PipelineSettings.from_sources(
        env_file=env_file,
        shell_env=os.environ,
    )

    account = os.getenv("CDK_DEFAULT_ACCOUNT")
    region = settings.aws_region or os.getenv("CDK_DEFAULT_REGION")
    if not account:
        raise ValueError(
            "CDK_DEFAULT_ACCOUNT is not set. Configure AWS credentials/profile before running CDK."
        )
    if not region:
        raise ValueError(
            "AWS_REGION is not set in .env and CDK_DEFAULT_REGION is not available."
        )

    aws_env = cdk.Environment(account=account, region=region)

    shared = PipelineSharedStack(
        app,
        f"{settings.stack_prefix}Shared",
        env=aws_env,
        settings=settings,
    )

    orchestration = PipelineOrchestrationStack(
        app,
        f"{settings.stack_prefix}Orchestration",
        env=aws_env,
        settings=settings,
        shared=shared,
    )
    orchestration.add_dependency(shared)

    app.synth()


if __name__ == "__main__":
    main()
