#!/usr/bin/env python3

##
## Runs Mustache template system using argv files, by terminal.
##

import sys, os, getopt
import chevron


def main(argv):
   fname_mustache = ''
   fname_json = ''
   outputfile = ''
   str_help = argv[0] + ' -t <template_file> -j <json_file> -o <outputfile>'
   flag_except=False
   
   try:
      opts, args = getopt.getopt(argv[1:],"ht:j:o:",["tpl=","json=","ofile="])
   except getopt.GetoptError:
      flag_except=True
   if len(argv) < 2:
      flag_except=True
   if flag_except and len(argv)>=1:
      print ("Please supply one or more arguments:\n"+str_help)
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
      elif opt in ("-j", "--json"):
         fname_json = arg
      elif opt in ("-o", "--ofile"):
         outputfile = arg

   if not os.path.isfile(fname_mustache):
     print ('ERR1. Template file not found: '+fname_mustache)
     sys.exit(2)
   elif not os.path.isfile(fname_json):
     print ('ERR1. JSON file not found: '+fname_json)
     sys.exit(2)
   
   result = chevron.main(fname_mustache,fname_json)
   
   if outputfile>'':
      print ('Input mustache file: ', fname_mustache)
      print ('Input json file: ', fname_json)
      print ('Output file: ', outputfile)
      with open(outputfile, "w") as text_file:
         text_file.write(result)
   else:
      print(result)

########

if __name__ == "__main__":
   main(sys.argv)


