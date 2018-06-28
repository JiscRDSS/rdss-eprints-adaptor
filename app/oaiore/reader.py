from lxml import etree
from oaipmh.metadata import MetadataReader
from oaipmh import common
from collections import defaultdict


class OREMetadataReader(MetadataReader):
    """	Adds additional field_types to the MetadataReader found in the 
	pyoai library to translate elements with attributes to dicts for 
	the OAI-ORE output. 
	""" 

    def _element_to_dict(self, element):
        """ Converts a childless etree.Element to a dict. 
        """
        d = {} 
        if element.attrib:
            d.update((k, v) for k, v in element.attrib.items())
        if element.text:
            text = t.text.strip()
            d['text'] = text
        return d

    def __call__(self, element):
        map = {}
        # create XPathEvaluator for this element
        xpath_evaluator = etree.XPathEvaluator(element, 
                                               namespaces=self._namespaces)
        
        e = xpath_evaluator.evaluate
        # now extra field info according to xpath expr
        for field_name, (field_type, expr) in list(self._fields.items()):
            if field_type == 'bytes':
                value = str(e(expr))
            elif field_type == 'bytesList':
                value = [str(item) for item in e(expr)]
            elif field_type == 'text':
                # make sure we get back unicode strings instead
                # of lxml.etree._ElementUnicodeResult objects.
                value = text_type(e(expr))
            elif field_type == 'textList':
                # make sure we get back unicode strings instead
                # of lxml.etree._ElementUnicodeResult objects.
                value = [text_type(v) for v in e(expr)]
            elif field_type == 'dict':
                value = [self._element_to_dict(v) for v in e(expr)]
            else:
                raise Error("Unknown field type: %s" % field_type)
            map[field_name] = value
        return common.Metadata(element, map)

oai_ore_reader = OREMetadataReader(
    fields={
        #'id':         ('textList', 'atom:entry/atom:id/text()'),
        'link':       ('dict', 'atom:entry/atom:link'), #needs more complex query
        #'published':  ('textList', 'atom:entry/atom:published/text()'),
        #'updated':    ('textList', 'atom:entry/atom:updated/text()'),
        #'title':    ('textList', 'atom:entry/atom:title/text()'),
        #'author_name':    ('textList', 'atom:entry/atom:author/atom:name/text()'),
    },
    namespaces={
	# v. http://www.openarchives.org/ore/1.0/atom#namespaces
        'atom': 'http://www.w3.org/2005/Atom', #Atom namespace
        'dc': 'http://purl.org/dc/elements/1.1/', #Dublin Core elements
        'dcterms': 'http://purl.org/dc/terms/', #Dublin Core terms
        'foaf': 'http://xmlns.com/foaf/0.1/', #FOAF vocabulary terms
        'ore': 'http://www.openarchives.org/ore/terms/', #ORE vocabulary terms
        'oreatom': 'http://www.openarchives.org/ore/atom/', #ORE Atom elements
        'rdf': 'http://www.w3.org/1999/02/22-rdf-syntax-ns#', #RDF vocabulary terms
        'rdfs': 'http://www.w3.org/2000/01/rdf-schema#', #RDF vocabulary terms
        }
)
