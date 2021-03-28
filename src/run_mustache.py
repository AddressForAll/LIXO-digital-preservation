#!/usr/bin/env python3

##
## Runs Mustache template system using argv files, by terminal.
##

import sys, os, getopt, pprint
import chevron
from csv import DictReader

def main(argv):
   fname_mustache = ''
   fname_json     = ''
   fname_csv      = ''
   outputfile     = ''
   tpl_inline     = ''
   str_help = "Use the filenames --tpl --json --csv --output or its prefixes:\n " \
              + argv[0] + " -t <template_file> -j <json_file> [-o <outputfile>]\n or\n " \
              + argv[0] + " -t <template_file> -c <csv_file> [-o <outputfile>]\n or\n " \
              + argv[0] + " --tpl_inline=\"etc\" etc"
   flag_except=False
   
   try:
      opts, args = getopt.getopt(argv[1:],"ht:j:c:o:",["tpl=","json=","csv=","output=","tpl_inline="])
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
      elif opt in ("-j", "--json"): # or yaml!
         fname_json = arg
      elif opt in ("-c", "--csv"):
         fname_csv = arg      
      elif opt in ("-o", "--ofile"):
         outputfile = arg
      elif opt=='--tpl_inline':
         tpl_inline = arg
         fname_mustache = '/tmp/run_mustache.mustache'
         f = open(fname_mustache, "w")
         f.write(tpl_inline)
         f.close()

   if fname_mustache=='' or not os.path.isfile(fname_mustache):
     print ('ERR1. Template file not found: '+fname_mustache)
     sys.exit(2)
   elif fname_csv=='' and fname_json=='':
     print ('ERR2. No input file found')
     sys.exit(2)   
   elif fname_csv>'' and not os.path.isfile(fname_csv):
     print ('ERR3. CSV file not found: '+fname_csv)
     sys.exit(2)
   elif fname_json>'' and not os.path.isfile(fname_json):
     print ('ERR3. JSON file not found: '+fname_json)
     sys.exit(2)


   if fname_csv>'':
      with open(fname_csv, 'r') as read_obj:
          dict_reader = DictReader(read_obj)
          list_of_dict = list(dict_reader)
          # print(list_of_dict)
          with open(fname_mustache, 'r') as tpl:
              result = chevron.render( tpl, list_of_dict )
   else:
      result = chevron.main( fname_mustache, fname_json )
   
   if outputfile>'':
      print ('Input mustache file: ', fname_mustache)
      if fname_csv>'':
         print ('Input CSV file: ', fname_csv)
      else:
         print ('Input JSON file: ', fname_json)      
      print ('Output file: ', outputfile)
      with open(outputfile, "w") as text_file:
         text_file.write(result)
   else:
      print(result)

###

if __name__ == "__main__":
   main(sys.argv)
