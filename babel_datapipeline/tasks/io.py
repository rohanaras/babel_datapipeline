import luigi
import luigi.s3 as s3
import datetime
from babel_datapipeline.tasks.recommenders import EFTask


class LocalTargetInputs(luigi.ExternalTask):
    def output(self):
        return luigi.file.LocalTarget(path='local_raw_targets/aminer.paper')


class AminerS3Targets(luigi.Task):
    def output(self):
        s3client = s3.S3Client()
        gformat = luigi.format.GzipFormat()
        return s3.S3Target(path='S3://citation-databases/Aminer/raw/aminer.paper.gz',
                           format=gformat, client=s3client)


class DynamoOutputTask(luigi.Task):
    date = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        return EFTask(date=self.date)

    def run(self):
        from babel_datapipeline.database.transformer import main
        main('aminer', open(self.input()[1].path, 'r'), open(self.input()[0].path, 'r'), 'localhost', create=True,
             flush=True)