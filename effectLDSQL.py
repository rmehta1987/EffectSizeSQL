import psycopg2
import os
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy import Table, Column, Integer, String, MetaData
import subprocess # for running plink
import re
import numpy as np

def filter_VCF_with_BCF(path_to_files: str, path_to_bcf: str, filter_args: dict, file_name_out: str):
    """
        Grabs SNPS based on filter using BCFtools
        Inpsired by: https://github.com/CGATOxford/cgat/blob/master/CGAT/GWAS.py
        Note for the user, this requires you to know what fields are available in the VCF file, everything 
        but annotations must have empty values, therefore the values and keys are also case sensitive
    Args:
        path_to_file (str ): File to vcf files
        path_to_plink (str): Location of bctools
        filter_args (dict): Map of filters, bcftools (this is only for extracting not filtering, )
            Example '%CHROM\t%POS\t%INFO/AC_fin\t%INFO/vep' -- filters out chromosome, position, info-field with Allele count of finnish population, and VEP annotations
            dictionary should be of {key (chromosome): {empty value}}
            {key (INFO}: {field}}
    """
    
    #location of plink /usr/bin/bcftools
    def execute_command(command):
        import shlex
        s = shlex.shlex(command, posix=True)
        s.commenters= ''
        s.whitespace_split = True
        b = list(s)
        b[3] = '\''+ b[3] + '\''  # hacky way to make sure the query is surrounded by single quotes
        b2 = ' '.join(b)
        process = subprocess.Popen(b2, shell=True, stderr=subprocess.PIPE)
        out, errs = process.communicate()
        check_returncode = process.poll()
        if check_returncode == 0:
            print("Process compleleted")
        else:
            print("BCFTOOLs failed")

    create_args = ''
    
    for key in filter_args:
        if key == 'INFO':
            for value in filter_args[key]:
                create_args += '%{}/{}\t'.format(key, value)
        else:
            create_args += '%{}\t'.format(key)
    
    create_args += '\n' # add new line for every row filtered
        
    execute_command('{} query -f \'{}\' {} > {}'.format(path_to_bcf, create_args, path_to_files, file_name_out))
    
def filter_VCF_with_BCF(path_to_files: str, path_to_bcf: str, file_name_out: str):


vcf_file = '/home/rahul/PopGen/SimulationSFS/gnomAD_data/gnomad.exomes.r2.1.1.sites.1.vcf.bgz'
out_file = 'ac_non_cancer_fin_bcftools_vep_full.vcf'
bcf_dict = {'CHROM': '', 'POS': '', 'ID': '', 'REF': '', 'ALT': '', 'INFO': ['non_cancer_AC_fin', 'vep']}
bcf_loc = '/usr/bin/bcftools'

filter_VCF_with_BCF(vcf_file, bcf_loc, bcf_dict, out_file)
'''

try:
    USER = 'rahul'
    PASS = 'plight'
    HOST = '98.63.194.68'
    PORT = 5432
    DB = 'ukheight'
    # url needs to start with postgresql:// instead of postgres:// 
    # see https://stackoverflow.com/questions/62688256/sqlalchemy-exc-nosuchmoduleerror-cant-load-plugin-sqlalchemy-dialectspostgre
    db_string = f"postgresql://{USER}:{PASS}@{HOST}:{PORT}/{DB}"  
    engine = create_engine(db_string)
    meta=MetaData()
    items = Table('height', meta, Column('Chromosome', String, primary_key = True), Column('Position', Integer),)
    items = Table('gnomadNFE', meta, Column('Chromosome', String, primary_key = True), Column('Position', Integer),)
    meta.create_all(engine)
    print(engine.table_names())

except Exception as e:
    print("Uh oh, can't connect. Invalid dbname, user or password?")
    print(e)

'''