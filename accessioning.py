import os.path

import luigi
from shellout import shellout_no_stdout

__author__ = 'Cristina Yenyxe Gonzalez Garcia'


class LoadLastAccession(luigi.Task):
    """
    Given a study prefix, return the last accession generated for that study (stored in EVAPRO).
    """

    def requires(self):
        return []

    def run(self):
        # TODO
        return []

    def output(self):
        return self.input()


class VariantsAccessioning(luigi.Task):
    """
    Run the OpenCGA variant accessioning tool, whose options are (* are mandatory):

    * -i, --input
             File to annotation with accession IDs
      -o, --outdir
             Directory where the output file will be saved
      -p, --prefix
             Accession IDs prefix
      -r, --resume-from-accession
             Starting point to generate accessions (will not be included)
    * -s, --study-alias
             Unique ID for the study where the file is classified (used for prefixes)
    """

    # TODO Possibly implement a FileParameter or PathParameter class?
    file = luigi.Parameter(description='Input VCF file to process and load')
    vcf_dir = luigi.Parameter(description='Folder for storage of EVA VCF files')
    study_prefix = luigi.Parameter(description='Prefix identifying the study in variant accession IDs')
    # last_accession = luigi.Parameter(default=None)

    def requires(self):
        return LoadLastAccession()

    def run(self):
        # Simplest command-line
        command = '/home/cyenyxe/appl/opencga/opencga create-accessions -i {input} -p ess -s {prefix} -o {outdir}'
        kwargs = {'input': self.file,
                  'prefix': self.study_prefix,
                  'outdir': self.vcf_dir}

        # Fill optional arguments
        # TODO Use last_accession read from EVAPRO
        if self.last_accession is not None and len(self.last_accession) > 0:
            command += ' -r {resume}'
            kwargs['resume'] = self.last_accession

        # Launch tool
        shellout_no_stdout(command, **kwargs)

    def output(self):
        (filename, extension) = os.path.splitext(os.path.basename(self.file))
        print 'Path to accessioned file = ' + luigi.LocalTarget(self.vcf_dir + filename + '_accessioned' + extension).fn
        return luigi.LocalTarget(self.vcf_dir + filename + '_accessioned' + extension)


class SaveLastAccession(luigi.Task):

    file = luigi.Parameter(description='Input VCF file to process and load')
    vcf_dir = luigi.Parameter(description='Folder for storage of EVA VCF files')
    study_prefix = luigi.Parameter(description='Prefix identifying the study in variant accession IDs')

    def requires(self):
        return VariantsAccessioning(self.file, self.vcf_dir, self.study_prefix)

    def run(self):
        # TODO
        return []

    def output(self):
        return self.input()

if __name__ == '__main__':
    luigi.run()
