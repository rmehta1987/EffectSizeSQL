import psycopg2
import os
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy import Table, Column, Integer, String, MetaData
import subprocess # for running plink
import re
import numpy as np
import shlex
from sqlalchemy.ext.automap import automap_base

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
    
def unrollVEP(path_to_files: str, path_to_bcf: str, file_name_out: str):
    """Unrolls the VEP field into separate columns

    Args:
        path_to_files (str): where the filtered vcf/file is 
        path_to_vcf (str): need the path to original vcf file so we can see the header of the vcf to extract VEP field 
        (bcftools view -h gnomad.exomes.r2.1.1.sites.1.vcf.bgz | grep -i 'ID=vep')
        path_to_bcf (str): location of bcftools
        file_name_out (str): output of unrolled and filtered VEP file
    """    

    #location of plink /usr/bin/bcftools
    def execute_command(command):
        process = subprocess.Popen(command, shell=True, stdout = subprocess.PIPE, stderr=subprocess.PIPE)
        out, errs = process.communicate()
        check_returncode = process.poll()
        if check_returncode == 0:
            print("Found header for vep")
        else:
            raise Exception("Something went wrong: {}".format(errs))
        return out
    
    out = execute_command("{} view -h {} | grep -i \'ID=vep\'".format(path_to_bcf, path_to_files)).decode('utf-8')
    # Get everything after keyword format in field and before ending double quote example: (##INFO..,Description="....Format: ....|Lof_info") -- note the space
    # Regex will get "....|Lof_info
    regex = r"(?<=Format: ).*?(?=\">)"
    match = re.findall(regex, out) 
    assert len(match) == 1, print("There should only be one large VEP field, multiple found {} {}".format(len(match), match))

    # Begin to unroll
    s = match[0].split('|')

    return s

Base = automap_base()
'''
class gnomadData(Base):

    def __init__(self, name_of_table, bcf_dict):
        super(Base, self).__init__
        self.__table__name = name_of_table

'''

vcf_file = '/home/rahul/PopGen/gnomAD_data/gnomad.exomes.r2.1.1.sites.1.vcf.bgz'
out_file = 'ac_non_cancer_fin_bcftools_vep_full.vcf'
bcf_dict = {'CHROM': '', 'POS': '', 'ID': '', 'REF': '', 'ALT': '', 'INFO': ['non_cancer_AC_fin', 'vep']}
bcf_loc = '/usr/bin/bcftools'

# filter_VCF_with_BCF(vcf_file, bcf_loc, bcf_dict, out_file)
unrollVEP(vcf_file, bcf_loc, out_file)

'''

try:
    USER = 'rahul'
    PASS = 'plight'
    HOST = '98.63.194.68'
    PORT = 5432
    DB = 'vcf'
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