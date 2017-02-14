from snorkel import *
from snorkel.parser import *
import os, io
import glob
from itertools import izip_longest
import unicodedata
import string
from StringIO import *

from pdfminer.converter import *
from pdfminer.layout import *
from pdfminer.pdfparser import *
from pdfminer.pdfdocument import PDFDocument
from pdfminer.pdfpage import PDFPage
from pdfminer.pdfpage import PDFTextExtractionNotAllowed
from pdfminer.pdfinterp import PDFResourceManager
from pdfminer.pdfinterp import PDFPageInterpreter
from pdfminer.pdfdevice import PDFDevice


def useCDRdata(session):
    from snorkel.parser import XMLMultiDocParser
    from snorkel.parser import SentenceParser
    
    xml_parser = XMLMultiDocParser(
        path='snorkel/tutorials/disease_tagging/data/CDR_TrainingSet.BioC.xml',
        doc='.//document',
        text='.//passage/text/text()',
        id='.//id/text()')
    sent_parser = SentenceParser()
    
    cp = CorpusParser(doc_parser=xml_parser, sent_parser=sent_parser)
    train_corpus = cp.parse_corpus(name='BD Training', session=session)
    session.add(train_corpus)
    session.commit()
    
    # add training and dev corpuses 
    cp.doc_parser.path = 'snorkel/tutorials/disease_tagging/data/CDR_DevelopmentSet.BioC.xml'
    dev_corpus = cp.parse_corpus(name='BD Development', session=session)
    session.add(dev_corpus)
    session.commit()
     
    cp.doc_parser.path = 'snorkel/tutorials/disease_tagging/data/CDR_TestSet.BioC.xml'
    test_corpus = cp.parse_corpus(name='BD Test', session=session)
    session.add(test_corpus)
    session.commit()

    return session

def myXMLdata(session):
    from snorkel.parser import XMLMultiDocParser
    from snorkel.parser import SentenceParser
    
    xml_parser = XMLMultiDocParser(
        path='articles/mined',
        doc='//article',
        text='.//front/article-meta/abstract/text()',
        id='.//front/article-meta/article-id/text()')
    sent_parser = SentenceParser()
    
    cp = CorpusParser(doc_parser=xml_parser, sent_parser=sent_parser)
    train_corpus = cp.parse_corpus(name='BD Training', session=session)
    session.add(train_corpus)
    session.commit()
    
    # add training and dev corpuses 
    #cp.doc_parser.path = 'snorkel/tutorials/disease_tagging/data/CDR_DevelopmentSet.BioC.xml'
    #dev_corpus = cp.parse_corpus(name='BD Development', session=session)
    #session.add(dev_corpus)
    #session.commit()
     
    #cp.doc_parser.path = 'snorkel/tutorials/disease_tagging/data/CDR_TestSet.BioC.xml'
    #test_corpus = cp.parse_corpus(name='BD Test', session=session)
    #session.add(test_corpus)
    #session.commit()

    return session    
    
    
    
    
def grouper(n, iterable, fillvalue=None):
    "Collect data into fixed-length chunks or blocks"
    # grouper(3, 'ABCDEFG', 'x') --> ABC DEF Gxx
    args = [iter(iterable)] * n
    return izip_longest(fillvalue=fillvalue, *args)

def pdfToText(folder, flag=None):
    root = os.getcwd()
    print os.getcwd()
    ocean = os.listdir(folder)
    
    # initial cleanup of .pdf.txt files if already generated
    # needed? or is rewriting fine?
    if flag == "clean":
        for fish in ocean:
             if(fish.endswith(".pdf.txt")):
                    os.remove(root + '/articles/' + fish)
                    
    # redundancy check: decide whether conversion is needed (and for which files)
    else:
        os.chdir(folder)
        
        pFiles = set(glob.glob("*.pdf"))
        pNames = set()
        for p in pFiles:
            pNames.add(os.path.splitext(p)[0].replace(".","").replace("-",""))

        ptFiles = set(glob.glob("*.pdf.txt"))
        ptNames = set()
        for pt in ptFiles:
            ptNames.add(os.path.splitext(os.path.splitext(pt)[0])[0].replace(".","").replace("-",""))
            
        print "\npdfs: ", len(pNames), pNames
        print "\npdftxts: ", len(ptNames), ptNames

        # rough set equivalence (or for some reason, more pt's than p's)
        if pNames == ptNames or len(ptFiles) > len(pFiles):
            print "\nAll files converted from .pdf --> .pdf.txt!\nAborting conversion process... "
            os.chdir(root)
            return
        # less pt's than p's
        else: 
            print "\nDiscrepancy found... Converting all docs for safety\n"
            # find set difference and convert those specific files
            os.chdir(root)
            
    # conversion: .pdf --> .pdf.txt    
    for fish in ocean:
        if(fish.endswith(".pdf")): # do we need this now?
            print fish
            oldFish = fish
            
            output = StringIO()
            manager = PDFResourceManager()
            converter = TextConverter(manager, output, laparams=LAParams())
            interpreter = PDFPageInterpreter(manager, converter)

            infile = file(folder + "/" + fish, 'rb')
            for page in PDFPage.get_pages(infile, set()):
                interpreter.process_page(page)
            infile.close()
            converter.close()
            text = output.getvalue()
            
            # cleaning up filename periods so regex works correctly when creating unique stable-ids
            # (converting extra "." characters to "-" characters).
            periodLocs = [i for i, ltr in enumerate(fish) if ltr == '.']
            if len(periodLocs) >= 2:
                toRemove = periodLocs[:-1]
                for periodLoc in toRemove:
                    fish = fish[:periodLoc] + '-' + fish[periodLoc+1:]
            print fish, "\n"
            
            # renaming pdfs to new names for consistency
            #print oldFish, fish
            os.rename(folder + "/" + oldFish, folder + "/" + fish)

            with open(folder + "/" + fish + ".txt", "w+") as f:
                f.write(text)

                
def parseDoc(_filename, session):
    original_path = os.getcwd()
    print original_path
    # creating .pdf.txt files to parse
    pdfToText(_filename)

    # File Parser
    #------------
    if os.path.isfile(original_path + "/" + _filename):
        print "Need to set up file parse!"
        
        filename = _filename
        text = ""

        with open(_filename, "rb") as f:
            text = f.read()
            printable = set(string.printable)
            text = filter(lambda x: x in printable, text)
            text = text.encode('utf-8')
            f.close()
        with open(_filename, "w+") as f:
            f.write(text)
            f.close()
        allSentences = []

        try:
            #----- LINE OF INTEREST -----
            sent_parser = SentenceParser()
            doc_parser = TextDocParser(filename)
            cp = CorpusParser(doc_parser, sent_parser)
            fCorpus = cp.parse_corpus(name='FileCorpus', session=session)
            
            allSentences = set()
            for document in fCorpus:
                for sentence in document.sentences:
                    allSentences.add(sentence)
            #print allSentences

        except(ValueError):
            n = 100
            with open(filename, 'rb+') as f:
                small_file_list = []
                for i, g in enumerate(grouper(n, f, fillvalue=''), 1):
                    with open(filename + '_{0}'.format(i * n), 'w') as fout:
                        small_file_list.append(filename + '_{0}'.format(i * n))
                        fout.writelines(g)
            
            print small_file_list
            for small_file in small_file_list:
                try:
                    #----- LINE OF INTEREST -----
                    sent_parser = SentenceParser()
                    doc_parser = TextDocParser(small_file)
                    cp = CorpusParser(doc_parser, sent_parser)
                    fCorpus = cp.parse_corpus(
                        name='FileCorpus', session=session)

                    sentences = set()
                    for document in fCorpus:
                        for sentence in document.sentences:
                            sentences.add(sentence)

                except(ValueError):
                    sentences = maxParseDoc(small_file, 50)
                for sentence in sentences:
                    allSentences.append(sentence)
                    
        return allSentences

    # Directory Parser
    #-----------------
    elif os.path.isdir(original_path + "/" + _filename):
        print os.getcwd()
        # create list of pdf.txt files to parse
        text_file_list = os.listdir(_filename)
        temp_list = []
        for file in text_file_list:
            if (file.endswith(".pdf.txt")):
                temp_list.append(file)
        text_file_list = temp_list
        
        print "\n text_file_list:"
        print text_file_list
        print "\n"
        
        # pre-processing - break each pdf.txt file into smaller files
        for filename in text_file_list:
            text = ""
            
            #with io.open(_filename + filename, mode='rb', encoding='utf-8') as f:
            with open(_filename + filename, "rb") as f:
                text = f.read()
                printable = set(string.printable)
                text = filter(lambda x: x in printable, text)
                #text = text.encode('utf-8', "ignore") #text = unicode(text)
                #text = text.decode('cp1252').encode('utf-8')
                text = unicode(text, errors='replace').encode('utf-8')
                #text = encoding.smart_str(text, encoding='utf-8', errors='ignore')
                f.close()
            
            #with io.open(_filename + filename, mode='w+', encoding='utf-8') as f:
            with open(_filename + filename, "w+") as f:
                f.write(text)
                f.close()
            
            n = 200    # safe line count threshold/document to avoid crashing
            
            #with io.open(_filename + filename, mode='rb+', encoding='utf-8') as f:
            with open(_filename + filename, 'rb+') as f:
                # print os.getcwd()
                print "dir: " + _filename + filename

                # writing all the subdivided parsed files into subdirectory
                for i, g in enumerate(grouper(n, f, fillvalue=''), 1):
                    #g = [el.encode('utf-8', "ignore") for el in g] #[unicode(el, "ignore") for el in g]
                    periodLoc = filename.index('.')
                    #with io.open(_filename + 'parsed_text/' + filename[0:periodLoc] + '_{0}'.format(i * n) + filename[periodLoc:], mode='w', encoding='utf-8') as fout:
                    with open(_filename + 'parsed_text/' + filename[0:periodLoc] + '_{0}'.format(i * n) + filename[periodLoc:], 'w') as fout:

                        fout.writelines(g)
                        fout.close()           
                f.close()

        # create list of all small files to analyze and collect names of files in cwd
        root = os.getcwd()
        parsed_dir_from_root = '/' + _filename + 'parsed_text/'
        parsed_dir = root + parsed_dir_from_root
        os.chdir(parsed_dir)

        small_file_list = glob.glob("*")
        print " \n small_file_list:"
        print small_file_list
        print "\n"

        # switch back to root for DB entry
        os.chdir(root)
        #print "cwd: " + os.getcwd()
        
        # parse small files into documents
        allSentences = []
        sent_parser = SentenceParser()
        doc_parser = TextDocParser('articles/parsed_text/', encoding='windows-1252')
        cp = CorpusParser(doc_parser=doc_parser, sent_parser=sent_parser)
        dCorpus = cp.parse_corpus(name='DocCorpus', session=session)
        
        sentences = set()
        for document in dCorpus:
            for sentence in document.sentences:
                sentences.add(sentence)
        #print sentences
        
        for sentence in sentences:
            allSentences.append(sentence)
        #print allSentences
        
        os.chdir(original_path)
        return allSentences


def maxParseDoc(_filename, parsePerLine, session):
    
    # clear if corpus exists
    # try:
    #    session.delete(mCorpus)
    # except UnboundLocalError:
    #    pass
        
    filename = _filename
    allSentences = []
    try:
        #----- LINE OF INTEREST -----
        sent_parser = SentenceParser()
        doc_parser = TextDocParser(filename)
        cp = CorpusParser(doc_parser, sent_parser)

        mCorpus = cp.parse_corpus(name='MaxCorpus', session=session)
        #session.add(mCorpus)
        #session.commit()
            
        # allSentences = cp.get_contexts()
        sentences = set()
        for document in mCorpus:
            for sentence in document.sentences:
                sentences.add(sentence)

    except:
        n = parsePerLine
        with open(filename, 'rb+') as f:
            small_file_list = []
            print os.getcwd()
            for i, g in enumerate(grouper(n, f, fillvalue=''), 1):
                with open('too_large/' + filename + '_{0}'.format(i * n), 'w') as fout:
                    print filename + '_{0}'.format(i * n)
                    small_file_list.append(filename + '_{0}'.format(i * n))
                    fout.writelines(g)

        print small_file_list
        for small_file in small_file_list:
            #----- LINE OF INTEREST -----
            sent_parser = SentenceParser()
            doc_parser = TextDocParser('too_large/' + small_file)
            cp = CorpusParser(doc_parser, sent_parser)

            mCorpus = cp.parse_corpus(name='Max Corpus', session=session)
            session.add(mCorpus)
            session.commit()
        
            # sentences = cp.get_contexts()
            sentences = set()
            for document in mCorpus:
                for sentence in document.sentences:
                    sentences.add(sentence)

            for sentence in sentences:
                allSentences.append(sentence)
                
    return allSentences
