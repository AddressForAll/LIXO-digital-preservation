#!/usr/bin/env python3

##
## Runs Mustache template system using argv files, by terminal.
##

import sys, os, getopt, pprint
import chevron
from csv import DictReader

## ## ##
## Mustache-input Standard Library:
#class MsiStdLib:
def items_setLast(x,lstLabel='last'):
  if x:  x[ len(x) - 1 ][lstLabel] = True
#msi = MsiStdLib()
def load_data(file, loaderName='SafeLoader'):
    if loaderName=='csv':
      print("em construcao CSV")
    else:
      try:
        import yaml
        loader = getattr(yaml, loaderName)  # not tested
        return yaml.load(file, Loader=loader)  # not tested
      except ImportError:
        import json
        return json.load(file)
## ## ##

def main(argv):
   fname_mustache =  fname_input  =  fname_input0  = ''
   partials_path = '/opt/gits/_a4a/digital-preservation-BR/src/maketemplates/'
   fname_input_csv  = ''
   outputfile     = ''
   tpl_inline     = ''
   json_inline    = ''
   str_help = "Use the filenames --tpl --input --csv --output --input0 --tplLast or its prefixes:\n " \
              + argv[0] + " -t <template_file> -i <input_file> [-o <outputfile>]\n or\n " \
              + argv[0] + " -t <template_file> -c <csv_file> [-o <outputfile>]\n or\n " \
              + argv[0] + " --tpl_inline=\"etc\" --json_inline=\"etc\" etc"
   flag_except=False
   #print(*argv)

   try:
      opts, args = getopt.getopt(argv[1:],"ht:i:c:o:",["tpl=","input=","input0=","csv=","output=","tpl_inline=","json_inline=","tplLast="])
   except getopt.GetoptError:
      flag_except=True
   if len(argv) < 2:
      flag_except=True
   if flag_except and len(argv)>=1:
      print ("Please supply one or more arguments\n"+str_help)
      sys.exit(2)
   elif flag_except:
      print ('!BUG ON ARGV!')
      sys.exit(2)

   for opt, arg in opts:
      if opt == '-h':
         print (str_help)
         sys.exit()
      elif opt in ("-t", "--tpl"):
         fname_mustache = arg
      elif opt in ("-1","--tplLast"):
         fname_mustacheLast = arg
      elif opt in ("-0","--input0"): # yaml
         fname_input0 = arg
      elif opt in ("-i", "--input"): # json or yaml!
         fname_input = arg
      elif opt in ("-c", "--csv"):
         fname_input_csv = arg
      elif opt in ("-o", "--ofile"):
         outputfile = arg
      elif opt=='--tpl_inline':
         tpl_inline = arg
         fname_mustache = '/tmp/run_mustache.mustache'
         f = open(fname_mustache, "w")
         f.write(tpl_inline)
         f.close()
      elif opt=='--input_inline':
         json_inline = arg
         fname_input  = '/tmp/run_mustache.json'
         f = open(fname_input, "w")
         f.write(json_inline)
         f.close()

   if fname_mustache=='' or not os.path.isfile(fname_mustache):
     print ('ERR1. Template file not found: '+fname_mustache)
     sys.exit(2)
   elif fname_input_csv=='' and fname_input=='':
     print ('ERR2. No input file found,  no csv=%s no x=%s' % (fname_input_csv,fname_input))
     sys.exit(2)
   elif fname_input_csv>'' and not os.path.isfile(fname_input_csv):
     print ('ERR3. CSV file not found: '+fname_input_csv)
     sys.exit(2)
   elif fname_input>'' and not os.path.isfile(fname_input):
     print ('ERR3. JSON or YAML file not found: '+fname_input)
     sys.exit(2)

   if fname_input0>'':
     fname2  = '/tmp/run_mustache.yaml'
     filenames = [fname_input0, fname_input]
     import fileinput
     with open(fname2, 'w') as fout, fileinput.input(filenames) as fin:
         for line in fin:
             fout.write(line)
     fout.close()
     fname_input = fname2

   if fname_mustacheLast>'':
     fname2  = '/tmp/run_mustache2.mustache'
     filenames = [fname_mustache, fname_mustacheLast]
     import fileinput
     with open(fname2, 'w') as fout, fileinput.input(filenames) as fin:
         for line in fin:
             fout.write(line)
     fout.close()
     fname_mustache = fname2

   if fname_input_csv>'':
      with open(fname_input_csv, 'r') as read_obj:
          listOfDict = list( DictReader(read_obj) )
          items_setLast(listOfDict)
   else:
      with open(fname_input, 'r') as read_obj:
          listOfDict = load_data(read_obj)
          items_setLast(listOfDict['files'])
   with open(fname_mustache, 'r') as tpl:
      result = chevron.render( tpl, listOfDict, partials_path, 'mustache' )

   if outputfile>'':
      print ('Input mustache file: ', fname_mustache)
      if fname_input_csv>'':
         print ('Input CSV file: ', fname_input_csv)
      else:
         print ('Input JSON ot YAML file: ', fname_input)
      print ('Output file: ', outputfile)
      with open(outputfile, "w") as text_file:
         text_file.write(result)
   else:
      print(result)

###

if __name__ == "__main__":
   main(sys.argv)
