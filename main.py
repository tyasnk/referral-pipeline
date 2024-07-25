import click
from pipeline import ReferralDataPipeline


@click.command()
@click.option(
    "-i",
    "--input-data-prefix-path",
    help="Directory contain tables needed. It can be S3 bucket or local directory",
    default="data",
)
@click.option(
    "-o",
    "--output-path",
    help="Output Path directory. It can be S3 bucket or local directory",
    default="result",
)
def main(
    input_data_prefix_path: str,
    output_path: str,
):
    pipeline = ReferralDataPipeline()
    pipeline.run(input_data_prefix_path=input_data_prefix_path, output_path=output_path)


if __name__ == "__main__":
    main()
