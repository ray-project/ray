import time
import os, tempfile
import glob
from datetime import datetime
from icecream import ic  # Never use print again: https://github.com/gruns/icecream
import csv
import re
import ray    
#import anyscale
import psutil 
import gc
import pandas as pd
#import dask.dataframe as dd
#pip install modin[all]
#from modin.config import Engine
#Engine.put("ray")
#import modin.pandas as pd

import RAKE # https://github.com/fabianvf/python-rake
from flair.data import Sentence        # https://github.com/flairNLP/flair
from flair.models import SequenceTagger

# python /Users/ben/trabajo/NLP/keyword-NER/keyword-NER-extractor.py






def main():
    # compute resources and setup
    setting = 'cluster'    # 'laptop'  or   'cluster'
    cpus = 2
    max_records = 50
    maxWordsPulled = 3  # used for keyword extraction
    language_model = 'ner-fast'  # try 'ner-large' when using anyscale workspace
    


    # refine parameters depending on setting
    if setting == 'laptop':
        dir = 'path_on_laptop'
    else:
        dir = '/home/ray/workspace-project-ben-ner/results/'
        max_records = 250000
        language_model = 'ner-large'   # size is 2.24GB
        cpus = psutil.cpu_count(logical=False)
    
    # input and output files
    curr_date = datetime.today().strftime('%Y-%m-%d')
    file_in = dir +  'sample-file-debug.csv'
    file_out = dir + 'sample-file-debug-keywords_out-' + curr_date + '.txt'
    os.system('rm ' + file_out)

    # In recent versions of Ray (>=1.5), ray.init() will automatically be called on the first use of a Ray remote API.
    # initialize language models: and place in the Ray Object Store
    #     tagger and rake
    # NER model options:  see "List of Pre-Trained Sequence Tagger Models", 
    #     here https://github.com/flairNLP/flair/blob/master/resources/docs/TUTORIAL_2_TAGGING.md
    tagger_ref = ray.put( SequenceTagger.load(language_model) )
    rake_ref =  ray.put( RAKE.Rake(RAKE.SmartStopList()) ) 
    

    # -----------------------------------------------------------------------------------------
    # NLP: text processing functions  (assumes each line is a document)
    # -----------------------------------------------------------------------------------------
    @ray.remote(num_cpus=cpus)
    def process_line_df_in(row):
        rake = ray.get(rake_ref)
        tagger = ray.get(tagger_ref)
        maxwords_in_phrase = 3
        dlm = '\t'
        l = row
        description = str(row['description']).strip()

        # sub functions -------------------------------------
        def extract_entities_in(tagger_in, sentence):
            # NER using Flair
            # tagger = SequenceTagger.load('ner-fast')
            # see  https://github.com/flairNLP/flair/blob/master/resources/docs/TUTORIAL_2_TAGGING.md
            tagger_in.predict(sentence)
            out_str = []
            for entity in sentence.get_labels('ner'):
                out_str.append(str(entity))
    
            out_str = ' // '.join(out_str)
            # format/prettify extracted entities 
            out_str = re.sub('Span\[\d+.\d+\]\:\s+', ' ', out_str)
            return out_str.strip().replace('"','')

        # keyword extraction with RAKE: extract up to 4 keywords
        def list_of_keywords(kw_rake):
            list_kw = [x[0] for x in kw_rake]

            l = list_kw[0:4]
            while len(l) < 4:
                l.append('')
            return l
        # end sub functions ---------------------------------
        
        # keyword extraction via RAKE
        text_in = description
        text_in.strip()
        kw_rake = rake.run(text_in, minCharacters = 1, maxWords = maxWordsPulled, minFrequency = 1)
        kw1, kw2, kw3, kw4 = list_of_keywords(kw_rake)

        # NER using flair
        sentence = Sentence(text_in)
        extracted_entities = extract_entities_in(tagger,sentence)

        # checks the amount of used memory and then calls the garbage collector
        # source: https://stackoverflow.com/questions/55749394/how-to-fix-the-constantly-growing-memory-usage-of-ray
        """
        Call the garbage collection if memory used is greater than 80% of total available memory.
        This is called to deal with an issue in Ray not freeing up used memory.
        """
        if psutil.virtual_memory().percent >= 0.8:
            gc.collect()
   

        # prepare output
        outlist = [description, kw1, kw2, kw3, kw4, extracted_entities]
        out_str = dlm.join(outlist)
        print(out_str)
        return out_str
    # End NLP line processor ------------------------------------------------------------------
    # -----------------------------------------------------------------------------------------

    # read input file - one line per document
    pdf = pd.read_csv(file_in,sep='\t', header=0)
    ic(list(pdf))
    pdf = pdf.head(max_records)     # this is actually a modin dataframe.
    ds = ray.data.from_pandas(pdf)
    del pdf


    # process documents
    start = time.time()
    taskcounter = 0
    while(taskcounter <= 0):
        outlist = ds.map( lambda x: process_line_df_in.remote(x) )
        taskcounter += 1
    
    end = time.time()
    ic('Data processing time = ', end - start, ' seconds')
    # Clean-up Large Language Models from object store
    del tagger_ref
    del rake_ref


    ic('Finishing processing documents .....')
    outlist_final = outlist.map_batches( lambda x: ray.get(x) ).to_pandas(limit=max_records)
    # save results  to dir
    try:
        print("Printing results to this directory:  " + dir)
        # print header later; no quotes; no index
        outlist_final.to_csv(file_out, sep='\t', header=False, index=False, quoting = csv.QUOTE_NONE, escapechar = ' ')
    except FileNotFoundError:
        print('FileNotFoundError: printing to ' + temp_dir)
        file_out2 = './sample-file-debug-keywords_out-' + curr_date + '.txt'  
        outlist_final.to_csv(file_out2, sep='\t', header=False, index=False, quoting = csv.QUOTE_NONE, escapechar = ' ')
        
    
    # print column headers for output file
    outlist_header = ['description', 'keyword1','keyword2','keyword3','keyword4', 'extracted_entities']
    out_header_tab = '\t'.join(outlist_header) + '\n'
    line = out_header_tab
    with open(file_out, 'r+') as file: 
        file_data = file.read() 
        file.seek(0, 0) 
        file.write(line + file_data)

    # exit gracefully
    gc.collect()
    ray.shutdown()

    # cleanup
    del outlist
    del outlist_final




if __name__== "__main__":
    ic("Start")
    start = time.time()
    main()
    end = time.time()
    ic('Total Run time = ', end - start, ' seconds')
    ic("done")

