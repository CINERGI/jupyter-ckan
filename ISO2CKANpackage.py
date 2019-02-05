
# coding: utf-8

# # Imports

# In[1]:


import json
import re
import pytz
import requests

import USGINharvestHelpers as imp

from dateutil.parser import parse
from pylons import config

#from ckanext.spatial.harvesters import CSWHarvester
#from ckanext.spatial.harvesters.base import guess_resource_format

#defined in this notebook
#from ckanext.harvest.usgin_xml_reader import USGINXmlMapping


# # USGIN ISO XML to CKAN package mapping

# In[2]:


# %load E:\GitHub\NGDS\ckanext-metadata-bku03232018\ckanext\harvest\usgin_xml_reader.py

#from ckanext.spatial.model import MappedXmlObject, MappedXmlDocument

#imported functions defined in harvested_metadata.py. Use only the basic foundation
# classes define by ckanext.spatial.

#  completely define the mapping usedfrom ckanext.spatial.model import ISOElement, 
#  ISODocument, ISOResponsibleParty \
#    , ISOBoundingBox, ISOKeyword, ISOReferenceDate, ISOUsage \
#    , ISOAggregationInfo, ISOCoupledResources, ISODataFormat \
#    , ISOResourceLocator, ISOBrowseGraphic
    
#This defines an alternate ISODocument class, based on that defined in spatial\model\
#  harvested_metadata.py. this mapping of ISO content is used to construct the USGIN 
#  extras.md_package ckanext-metadata/ckanext/harvest/usgin.py

#keys (element names) are based on USGINMetadataJSONSchemav3.0

class MappedXmlElement(imp.MappedXmlObject):
    namespaces = {}

    def __init__(self, name, search_paths=[], multiplicity="*", elements=[]):
        self.name = name
        self.search_paths = search_paths
        self.multiplicity = multiplicity
        self.elements = elements or self.elements

    def read_value(self, tree):
        values = []
        for xpath in self.get_search_paths():
            elements = self.get_elements(tree, xpath)
            # values = self.get_values(elements)
            values = values.append(self.get_values(elements))  #smr attempt to gather all values from search paths
            ''' only catches values on first xpath that has values; consider changing behavior to catch values on all xpaths, and then concatenate in fix_multiplicity if the properity is supposed to be single valued '''
            #if values:
            #    break
        return self.fix_multiplicity1(values)
    
    def fix_multiplicity1(self, values):
        '''
        When a field contains multiple values, concatenate values as strings.

        In the ISO19115 specification, multiplicity relates to:
        * 'Association Cardinality'
        * 'Obligation/Condition' & 'Maximum Occurence'
        '''
        if self.multiplicity == "0":
            # 0 = None
            if values:
                log.warn("Values found for element '%s' when multiplicity should be 0: %s",  self.name, values)
            return ""
        
        #smr add catch to flag elements that don't get processed
        elif self.multiplicity == "-1":
            # 0 = None
            if values:
                log.warn("Values found for element '%s' but these were not processed",  self.name, values)
            return ""
        elif self.multiplicity == "1":
            # 1 = Mandatory, maximum 1 = Exactly oneusgin
            # if more than one item in values array, concatenate them
            if not values:
                log.warn("Value not found for element '%s'" % self.name)
                return ''
            valueslist_str = '; '.join([str(mli) for mli in values]) # code snippet from
            # http://stackoverflow.com/questions/12453580/concatenate-item-in-list-to-strings-python
            return valueslist_str
        elif self.multiplicity == "*":
            # * = 0..* = zero or more
            return values
        elif self.multiplicity == "0..1":
            # 0..1 = Mandatory, maximum 1 = optional (zero or one)
            if values:
                valueslist_str = '; '.join([str(mli) for mli in values]) # code snippet from
            # http://stackoverflow.com/questions/12453580/concatenate-item-in-list-to-strings-python
                return valueslist_str
            else:
                return ""
        elif self.multiplicity == "1..*":
            # 1..* = one or more
            return values
        else:
            log.warning('Valid multiplicity not specified for element: %s',
                        self.name)
            return values


        
    def get_search_paths(self):
        if type(self.search_paths) != type([]):
            search_paths = [self.search_paths]
        else:
            search_paths = self.search_paths
        return search_paths

    def get_elements(self, tree, xpath):
        return tree.xpath(xpath, namespaces=self.namespaces)

    def get_values(self, elements):
        values = []
        if len(elements) == 0:
            pass
        else:
            for element in elements:
                value = self.get_value(element)
                values.append(value)
        return values

    def get_value(self, element):
        if self.elements:
            value = {}
            for child in self.elements:
                value[child.name] = child.read_value(element)
            return value
        elif type(element) == etree._ElementStringResult:
            value = str(element)
        elif type(element) == etree._ElementUnicodeResult:
            value = unicode(element)
        else:
            value = self.element_tostring(element)
        return value

    def element_tostring(self, element):
        return etree.tostring(element, pretty_print=False)


# ## USGIN ISO component classes

# In[3]:




class USGINISOElement(MappedXmlElement):
    # declare gml and gml3.2 because either one might show up in instances ...

    namespaces = {
       "gts": "http://www.isotc211.org/2005/gts",
       "gml": "http://www.opengis.net/gml",
       "gml32": "http://www.opengis.net/gml/3.2",
       "gmx": "http://www.isotc211.org/2005/gmx",
       "gsr": "http://www.isotc211.org/2005/gsr",
       "gss": "http://www.isotc211.org/2005/gss",
       "gco": "http://www.isotc211.org/2005/gco",
       "gmd": "http://www.isotc211.org/2005/gmd",
       "srv": "http://www.isotc211.org/2005/srv",
       "xlink": "http://www.w3.org/1999/xlink",
       "xsi": "http://www.w3.org/2001/XMLSchema-instance",
    }


#process CI_OnlineResource
class ISOResourceLocator(USGINISOElement):
    elements = [
USGINISOElement(
    name="linkURL",
    search_paths=[
        "gmd:linkage/gmd:URL/text()",
    ],
    multiplicity="1",
),
USGINISOElement(
    name="function",
    search_paths=[
        "gmd:function/gmd:CI_OnLineFunctionCode/@codeListValue",
    ],
    multiplicity="0..1",
),
       
#smr addition; CI_OnlineResource function is correlated with linkRelation
USGINISOElement(
    name="functionText",
    search_paths=[
        "gmd:function/gmd:CI_OnLineFunctionCode/text()",
    ],
    multiplicity="0..1",
),
USGINISOElement(
    name="functionCodeList",
    search_paths=[
        "gmd:function/gmd:CI_OnLineFunctionCode/@codeList",
    ],
    multiplicity="0..1",
),
# end SMR insert

USGINISOElement(
    name="linkTitle",
    search_paths=[
        "gmd:name/gco:CharacterString/text()",
    ],
    multiplicity="0..1",
),
USGINISOElement(
    name="linkDescription",
    search_paths=[
        "gmd:description/gco:CharacterString/text()",
    ],
    multiplicity="0..1",
),
USGINISOElement(
    name="linkOverlayAPI",
    search_paths=[
        "gmd:protocol/gco:CharacterString/text()",
    ],
    multiplicity="0..1",
),
    ]

# SMR addition; process CI_Address. These get concatenated into a
#  contactAddress string in USGIN JSON metadata
class ISOPostalAddress(USGINISOElement):
    elements = [
#delivery-point
USGINISOElement(
    name="delivery-point",
    search_paths=[
                "gmd:address/gmd:CI_Address/gmd:deliveryPoint/gco:CharacterString/text()",
    ],
    multiplicity="0..1",
),
#city        
USGINISOElement(
    name="city",
    search_paths=[
                "gmd:address/gmd:CI_Address/gmd:city/gco:CharacterString/text()",
    ],
    multiplicity="0..1",
),      
#administrative area       
USGINISOElement(
    name="administrative-area",
    search_paths=[
                "gmd:address/gmd:CI_Address/gmd:administrativeArea/gco:CharacterString/text()",
    ],
    multiplicity="0..1",
),
#postal code
USGINISOElement(
    name="postal-code",
    search_paths=[
                "gmd:address/gmd:CI_Address/gmd:postalCode/gco:CharacterString/text()",
            ],
    multiplicity="0..1",
),
#country
USGINISOElement(
    name="country",
    search_paths=[
                "gmd:address/gmd:CI_Address/gmd:country/gco:CharacterString/text()",
            ],
    multiplicity="0..1",
),         
    ]


# parse from CI_Contact    
class ISOContactInfo(USGINISOElement):
    elements = [
# email
USGINISOElement(
    name="contactEmails",
    search_paths=[
        "gmd:address/gmd:CI_Address/gmd:electronicMailAddress/gco:CharacterString/text()",
    ],
    multiplicity="0..*",  #  modified to account for multiple e-mails
),
#SMR addition 
# ignore facsimile numbers... 
# Telephone Voice
USGINISOElement(
    name="telephone-voice",
    search_paths=[
        "gmd:phone/gmd:CI_Telephone/gmd:voice/gco:CharacterString/text()",
    ],
    multiplicity="*",  # Modified to allow multiple phone number...
),

ISOPostalAddress(
    name = "postal-address",
    search_paths=[
        "gmd:address/gmd:CI_Address",
    ],
    multiplicity="0..1",
),    
# end smr addition 
		# contact onlineResource (link)
ISOResourceLocator(
    name="contact-link",
    search_paths=[
        "gmd:onlineResource/gmd:CI_OnlineResource",
    ],
    multiplicity="0..1",
),
    ]

class ISOContact(USGINISOElement):
# this class is for a contact--person or org, with contact info, but no role
# context is CI_ResponsibleParty
    elements = [
#individual name
USGINISOElement(
    name="personName",
    search_paths=[
        "gmd:individualName/gco:CharacterString/text()",
    ],
    multiplicity="0..1",
),
#organization name
USGINISOElement(
    name="organizationNames",
    search_paths=[
        "gmd:organisationName/gco:CharacterString/text()",
    ],
    multiplicity="0..1",
),
#position name
USGINISOElement(
    name="personPosition",
    search_paths=[
        "gmd:positionName/gco:CharacterString/text()",
    ],
    multiplicity="0..1",
),
#contact information; this needs further processing to make USGIN JSON
ISOContactInfo(
    name="contact-info",
    search_paths=[
        "gmd:contactInfo/gmd:CI_Contact",
    ],
    multiplicity="0..1",
),

    ]


    
    # smr addition end 
class ISOResponsibleParty(USGINISOElement):
# this class represents an Agent (contact, party) in a specific role
# conttext is the role name that has CI_ResponsibleParty as the role filler type
    elements = [
#individual name
ISOContact(
    name="contact",
    search_paths=[
        "gmd:CI_ResponsibleParty",
    ],
    multiplicity="0..1",
),
 
#responsible party role
USGINISOElement(
    name="agentRoleConceptURI",
    search_paths=[
        "gmd:CI_ResponsibleParty/gmd:role/gmd:CI_RoleCode/@codeListValue",
    ],
    multiplicity="1",
),
#smr  change multiplicity on role to 1 
#smr addition add element text and codeSpace
USGINISOElement(
    name="agentRolePrefLabel",
    search_paths=[
        "gmd:CI_ResponsibleParty/gmd:role/gmd:CI_RoleCode/Text()",
    ],
    multiplicity="0..1",
),
USGINISOElement(
    name="agentRoleVocabularyURI",
    search_paths=[
        "gmd:CI_ResponsibleParty/gmd:role/gmd:CI_RoleCode/@codelist",
    ],
    multiplicity="0..1",
), 
#end smr addition
    ]

    #handle MD_Format
class ISODataFormat(USGINISOElement):
    elements = [
USGINISOElement(
    name="name",
    search_paths=[
        "gmd:name/gco:CharacterString/text()",
    ],
    multiplicity="1",
),
USGINISOElement(
    name="version",
    search_paths=[
        "gmd:version/gco:CharacterString/text()",
    ],
    multiplicity="0..1",
),
#smr add to capture other text
USGINISOElement(
    name="other-format-info",
    search_paths=[
        "gmd:amendmentNumber/gco:CharacterString/text()",
        "gmd:specification/gco:CharacterString/text()",
        "gmd:fileDecompressionTechnique/gco:CharacterString/text()",
    ],
    multiplicity="0..*",
),
    ]

    
# handle CI_Date    maps to EventDateObject in USGIN JSON
class ISOReferenceDate(USGINISOElement):

    elements = [
USGINISOElement(
    name="eventTypeConceptLabel",
    search_paths=[
        "gmd:dateType/gmd:CI_DateTypeCode/@codeListValue",
        "gmd:dateType/gmd:CI_DateTypeCode/text()",
    ],
    multiplicity="1",
),

#smr add dateType codeList 
USGINISOElement(
    name="eventTypeVocabularyURI",
    search_paths=[
        "gmd:dateType/gmd:CI_DateTypeCode/@codeList",
    ],
    multiplicity="0..1",
),

 
USGINISOElement(
    name="eventDateTime",
    search_paths=[
        "gmd:date/gco:Date/text()",
        "gmd:date/gco:DateTime/text()",
    ],
    multiplicity="1",
),
    ]

# handles srv:SV_CoupledREsource/resource    
class ISOCoupledResources(USGINISOElement):
    #assumes that operatesOn is implemented as an xlink href. should log a warning if there is an inline MD_DataIdentification
    #this appears to be junk; the apiso.xsd implementation of service metadata does not follow the UML in ISO19119.  Leave here for now, but isn't processed into USGIN JSON. 
    elements = [
#smr fix multiplicities
USGINISOElement(
    name="title",
    search_paths=[
        "@xlink:title",
    ],
    multiplicity="0..1",
),
USGINISOElement(
    name="href",
    search_paths=[
        "@xlink:href",
    ],
    multiplicity="0..1",
),
USGINISOElement(
    name="uuid",
    search_paths=[
        "@uuidref",
    ],
    multiplicity="0..1",
),
#shouldn't have inline MD_DataIdenfication; multiplicity -1 will put warning in log 
USGINISOElement(
    name="coupled-inline-dataIdentification",
    search_paths=[
        "MD_DataIdentification",
    ],
    multiplicity="-1",
),
    ]

class ISOBoundingBox(USGINISOElement):

    elements = [
USGINISOElement(
    name="west",
    search_paths=[
        "gmd:westBoundLongitude/gco:Decimal/text()",
    ],
    multiplicity="1",
),
USGINISOElement(
    name="east",
    search_paths=[
        "gmd:eastBoundLongitude/gco:Decimal/text()",
    ],
    multiplicity="1",
),
USGINISOElement(
    name="north",
    search_paths=[
        "gmd:northBoundLatitude/gco:Decimal/text()",
    ],
    multiplicity="1",
),
USGINISOElement(
    name="south",
    search_paths=[
        "gmd:southBoundLatitude/gco:Decimal/text()",
    ],
    multiplicity="1",
),
    ]

#MD_BrowseGraphic
class ISOBrowseGraphic(USGINISOElement):  
    elements = [
USGINISOElement(
    name="browseGraphicName",
    search_paths=[
        "gmd:fileName/gco:CharacterString/text()",
    ],
    multiplicity="1",
),
USGINISOElement(
    name="browseGraphicDescription",
    search_paths=[
        "gmd:fileDescription/gco:CharacterString/text()",
    ],
    multiplicity="0..1",
),
USGINISOElement(
    name="browseGraphicResourceType",
    search_paths=[
        "gmd:fileType/gco:CharacterString/text()",
    ],
    multiplicity="0..1",
)
    ]


#MD_Keywords   maps to USGIN JSON resourceIndexTerms
class ISOKeyword(USGINISOElement):

    elements = [
USGINISOElement(
    name="keyword",
    search_paths=[
        "gmd:keyword/gco:CharacterString/text()",
    ],
    multiplicity="*",
),
USGINISOElement(
    name="keywordTypeLabelURI",
    search_paths=[
        "gmd:type/gmd:MD_KeywordTypeCode/@codeListValue",
        "gmd:type/gmd:MD_KeywordTypeCode/text()",
    ],
    multiplicity="0..1",
),

#smr add typeCode codelist 
USGINISOElement(
    name="keywordTypeVocabularyURI",
    search_paths=[
        "gmd:type/gmd:MD_KeywordTypeCode/@codeList",
    ],
    multiplicity="0..1",
),

# smr add thesaurus information 

USGINISOElement(
    name="keywordReferenceTitle",
    search_paths=[
        "gmd:thesaurusName/gmd:CI_Citation/gmd:title/gco:CharacterString/text()",
    ],
    multiplicity="0..1",
),

USGINISOElement(
    name="keywordReferenceIdentifier",
    search_paths=[
        "gmd:thesaurusName/gmd:CI_Citation/gmd:identifier/gmd:MD_Identifier/gmd:code/gco:CharacterString/text()",
    ],
    multiplicity="0..1",
),
# end thesaurus information 

# If more Thesaurus information is needed at some point, this is the
# place to add it
   ]


# MD_Usage   
class ISOUsage(USGINISOElement):

    elements = [
USGINISOElement(
    name="specificUsage",
    search_paths=[
        "gmd:specificUsage/gco:CharacterString/text()",
    ],
    multiplicity="0..1",
),
ISOResponsibleParty(
    name="specificUsageUserContact",
    search_paths=[
        "gmd:userContactInfo",
    ],
    multiplicity="0..1",
),

#smr add usageDateTime and  limitations
USGINISOElement(
    name="specificUsageLimitations",
    search_paths=[
        "gmd:userDeterminedLimitations/gco:CharacterString/text()",
    ],
    multiplicity="0..1",
),

USGINISOElement(
    name="specificUsageDateTime",
    search_paths=[
        "gmd:usageDateTime/gco:DateTime/text()",
    ],
    multiplicity="0..1",
),
   ]

    
#MD_AggregationInfo    
class ISOAggregationInfo(USGINISOElement):
    elements = [
USGINISOElement(
    name="relatedResourceLabel",
    search_paths=[
        "gmd:aggregateDatasetName/gmd:CI_Citation/gmd:title/gco:CharacterString/text()",
    ],
    multiplicity="0..1",
),
USGINISOElement(
    name="relatedResourceIdentifier",
    search_paths=[
        "gmd:aggregateDatasetIdentifier/gmd:MD_Identifier/gmd:code/gco:CharacterString/text()",
        "gmd:aggregateDatasetName/gmd:CI_Citation/gmd:identifier/gmd:MD_Identifier/gmd:code/gco:CharacterString/text()",
    ],
    multiplicity="0..1",
),
USGINISOElement(
    name="relationTypeLabel",
    search_paths=[
        "gmd:associationType/gmd:DS_AssociationTypeCode/text()",
        "gmd:associationType/gmd:DS_AssociationTypeCode/@codeListValue",
    ],
    multiplicity="0..1",
),
USGINISOElement(
    name="relationTypeConceptURI",
    search_paths=[
        "gmd:associationType/gmd:DS_AssociationTypeCode/@codeListValue",
    ],
    multiplicity="1",
),
USGINISOElement(
    name="relationTypeVocabularyURI",
    search_paths=[
        "gmd:associationType/gmd:DS_AssociationTypeCode/@codeList",
    ],
    multiplicity="0..1",
),
USGINISOElement(
    name="relatedInitiativeTypeConceptURI",
    search_paths=[
        "gmd:initiativeType/gmd:DS_InitiativeTypeCode/@codeListValue",
    ],
    multiplicity="0..1",
),
USGINISOElement(
    name="relatedInitiativeLabel",
    search_paths=[
        "gmd:initiativeType/gmd:DS_InitiativeTypeCode/text()",
    ],
    multiplicity="0..1",
),
USGINISOElement(
    name="relatedInitiativeTypeVocabularyURI",
    search_paths=[
        "gmd:initiativeType/gmd:DS_InitiativeTypeCode/@codeList",
    ],
    multiplicity="0..1",
),
   ]

#smr MD_DigitalTransferOptions
class ISOTransferOptions(USGINISOElement):
    elements = [
USGINISOElement(
    name="distributionUnit",
    search_paths=[
        "gmd:unitsOfDistribution/gco:CharacterString/text()",
    ],
    multiplicity="0..1",
),
USGINISOElement(
    name="digitalTransferSize",
    search_paths=[
        "gmd:transferSize/gco:Real/text()",
    ],
    multiplicity="0..1",
),
ISOResourceLocator(
    name="resourceAccessLinks",
    search_paths=[
        "gmd:online/gmd:CI_OnlineResource",
    ],
    multiplicity="*",
),

USGINISOElement(
    name="offlineMediumURI",
    search_paths=[
        "gmd:offLine/gmd:MD_Medium/gmd:name/gmd:MD_MediumNameCode/@codeListValue",
    ],
    multiplicity="0..1",
),
USGINISOElement(
    name="offlineMediumVocabularyURI",
    search_paths=[
        "gmd:offLine/gmd:MD_Medium/gmd:name/gmd:MD_MediumNameCode/@codeList",
    ],
    multiplicity="0..1",
),
USGINISOElement(
    name="offlineMediumNote",
    search_paths=[
        "gmd:offLine/gmd:MD_Medium/gmd:mediumNote/gco:CharacterString/text()",
        "gmd:offLine/gmd:MD_Medium/gmd:density/gco:Real/text()",
        "gmd:offLine/gmd:MD_Medium/gmd:densityUnits/gco:CharacterString/text()",
        "gmd:offLine/gmd:MD_Medium/gmd:volumes/gco:Integer/text()",
    ],
    multiplicity="0..*",
),
USGINISOElement(
    name="offlineMediumFormatURI",
    search_paths=[
        "gmd:offLine/gmd:MD_Medium/gmd:mediumFormat/gmd:MD_MediumFormatCode/@codeListValue",
    ],
    multiplicity="*",
),
USGINISOElement(
    name="offlineMediumFormatVocabularyURI",
    search_paths=[
        "gmd:offLine/gmd:MD_Medium/gmd:mediumFormat/gmd:MD_MediumFormatCode/@codeList",
    ],
    multiplicity="*",
),
    ]
    
#smr add    
# MD_StandardOrderProcess
class ISOOrderProcess(USGINISOElement):
    elements = [
USGINISOElement(
    name="fees",
    search_paths=[
        "gmd:fees/gco:CharacterString/text()",
    ],
    multiplicity="0..1",
),
USGINISOElement(
    name="accessInstructions",
    search_paths=[
        "gmd:orderingInstructions/gco:CharacterString/text()",
    ],
    multiplicity="0..1",
),
USGINISOElement(
    name="availableDateTime",
    search_paths=[
        "gmd:plannedAvailableDateTime/gco:DateTime/text()",
    ],
    multiplicity="0..1",
),
USGINISOElement(
    name="orderTurnAround",
    search_paths=[
        "gmd:turnaround/gco:CharacterString/text()",
    ],
    multiplicity="0..1",
),
    ]  
#smr add
class ISODistributorAccessOptions(USGINISOElement):
    elements = [
ISOResponsibleParty(
    name="distributorContact",
    search_paths=[
        "gmd:distributorContact",
    ],
    multiplicity="1",
),
ISODataFormat(
    name="distributorFormat",
    search_paths=[
        "gmd:distributorFormat/gmd:MD_Format",
    ],
    multiplicity="*",
),
ISOTransferOptions(
    name="distributorDigitalTransferOption",
    search_paths=[
        "gmd:distributorTransferOptions/gmd:MD_DigitalTransferOptions",
    ],
    multiplicity="*",
),
ISOOrderProcess(
    name="distributorAccessInstructions",
    search_paths=[
        "gmd:distributionOrderProcess/gmd:MD_StandardOrderProcess",
    ],
    multiplicity="*",
),   
    ]


#MD_Constraints, used for metadatConstraints and resourceConstraints
class ISOConstraints(USGINISOElement):
    elements = [
USGINISOElement(
    name="useLimitation",
    search_paths="gmd:MD_Constraints/gmd:useLimitation/gco:CharacterString/text()",
    multiplicity="*",
),
       USGINISOElement(
    name="legalUseLimitation",
    search_paths="gmd:MD_LegalConstraints/gmd:useLimitation/gco:CharacterString/text()",
    multiplicity="*"
		),
USGINISOElement(
    name="legalOtherRestrictionConstraints",
    search_paths="gmd:MD_LegalConstraints/gmd:otherConstraints/gco:CharacterString/text()",
    multiplicity="*",
),
USGINISOElement(
    name="legalAccessRestrictionCode",
    search_paths=[
        "gmd:MD_LegalConstraints/gmd:accessConstraints/gmd:MD_RestrictionCode/@codeListValue",
        "gmd:MD_LegalConstraints/gmd:accessConstraints/gmd:MD_RestrictionCode/text()",
    ],
    multiplicity="*",
),
USGINISOElement(
    name="legalUseRestrictionCode",
    search_paths=[
        "gmd:MD_LegalConstraints/gmd:useConstraints/gmd:MD_RestrictionCode/@codeListValue",
        "gmd:MD_LegalConstraints/gmd:useConstraints/gmd:MD_RestrictionCode/text()",
    ],
    multiplicity="*",
),
USGINISOElement(
    name="securityUseLimitation",
    search_paths="gmd:MD_SecurityConstraints/gmd:useLimitation/gco:CharacterString/text()",
    multiplicity="*",
),
USGINISOElement(
    name="securityClassificationCode",
    search_paths=[
        "gmd:MD_SecurityConstraints/gmd:classification/gmd:ClassificationCode/@codeListValue",
        "gmd:MD_SecurityConstraints/gmd:classification/gmd:ClassificationCode/text()",
    ],
    multiplicity="0..1",
),
USGINISOElement(
    name="securityUserNote",
    search_paths="gmd:MD_SecurityConstraints/gmd:userNote/gco:CharacterString/text()",
    multiplicity="0..1",
),
USGINISOElement(
    name="securityClassificationSystem",
    search_paths="gmd:MD_SecurityConstraints/gmd:classificationSystem/gco:CharacterString/text()",
    multiplicity="0..1",
),
USGINISOElement(
    name="securityHandlingDescription",
    search_paths="gmd:MD_SecurityConstraints/gmd:handlingDescription/gco:CharacterString/text()",
    multiplicity="0..1",
),
USGINISOElement(
    name="constraintRestrictionCodelist",
    search_paths=[
        "gmd:MD_LegalConstraints/gmd:accessConstraints/gmd:MD_RestrictionCode/@codeList",
        "gmd:MD_LegalConstraints/gmd:useConstraints/gmd:MD_RestrictionCode/@codeList",
        "gmd:MD_SecurityConstraints/gmd:classification/gmd:ClassificationCode/@codeList",
    ],
    multiplicity="*",
)
	]

# gmd:MD_MaintenanceInformation
class ISOMaintenance(USGINISOElement):
    elements=[
       #updateFrequency
USGINISOElement(
    name="maintenanceFrequencyURI",
    search_paths=[
        "gmd:maintenanceAndUpdateFrequency/gmd:MD_MaintenanceFrequencyCode/@codeListValue",
        "gmd:maintenanceAndUpdateFrequency/gmd:MD_MaintenanceFrequencyCode/text()",
    ],
    multiplicity="1",
),
USGINISOElement(
    name="maintenanceFrequencyVocabularyURI",
    search_paths=[
        "gmd:maintenanceAndUpdateFrequency/gmd:MD_MaintenanceFrequencyCode/@codeList",
    ],
    multiplicity="0..1",
),
USGINISOElement(
    name="dateNextUpdate",
    search_paths=[
        "gmd:dateOfNextUpdate/gco:Date",
        "gmd:dateOfNextUpdate/gco:DateTime",
    ],
    multiplicity="0..1",
),
USGINISOElement(
    name="maintenanceInterval",
    search_paths=[
        "gmd:userDefinedMaintenanceFrequencey/gts:TM_PeriodDuration",
    ],
    multiplicity="0..1",
),
USGINISOElement(
    name="maintenanceInterval",
    search_paths=[
        "gmd:userDefinedMaintenanceFrequencey/gts:TM_PeriodDuration",
    ],
    multiplicity="0..1",
),

#maintenanceNote; concatenate all the other stuff; 
#updateScopeDescription is a collection of attributesinstance, datasets, other, not clear what
#might show up there; hopefully it gets converted to some kind of usable text...
USGINISOElement(
    name="MaintenanceNote",
    search_paths=[
        "gmd:maintenanceNote/gco:CharacterString/text()",
        "gmd:updateScope/gmd:MD_ScopeCode/@codeListValue",
        "gmd:updateScopeDescription/gmd:MD_ScopeDescription/text()"
    ],
    multiplicity="0..*",
),
ISOResponsibleParty(
    name="maintenanceContacts",
    search_paths=[
        "gmd:contact",
    ],
    multiplicity="0..*",
),

    ]

    


# # USGIN main dictionary: USGINXmlMapping class

# In[4]:


# this is the main dictionary that gets used by usgin.py to 
# construct the extras.md_package JSON object for USGIN metadata handling
# note: the operations stuff in sv_serviceIdentification is not handled.
# the big show--putting it all together in one dictionary
class USGINXmlMapping(imp.MappedXmlDocument):
    ''' this class constructs a javascript object withh all the ISO content elements 
    called by ckanext/spatiaL/harvesters/base.py line 512-517 
    updated by SMR 2015-10-11 '''
    
         #  new mapping by SMR to ISO19115-2 and 19110, using USING metadata JSON v3.0
    # 2016-01-21
    


# ## Metadata elements
# language, character set, parent identifier, Hierarchy level, standard name, Metadata contacts, metadata date

# In[5]:


elements = [
    #file identifier (identifies the metadata record, not the described resoruce)
    USGINISOElement(
        name="metadataIdentifier",
        search_paths="gmd:fileIdentifier/gco:CharacterString/text()",
        multiplicity="0..1",
    ),
    #language
    USGINISOElement(
        name="metadataLanguageCode",
        search_paths=[
            "gmd:language/gmd:LanguageCode/@codeListValue",
            "gmd:language/gco:CharacterString/text()"
        ],
        multiplicity="0..1",
    ),   
    # language codelist 
    USGINISOElement(
        name="metadataLanguageCodeList",
        search_paths=[
            "gmd:language/gmd:LanguageCode/@codeList",
        ],
        multiplicity="0..1",
    ),

    USGINISOElement(
        name="metadataCharacterSet",
        search_paths=[
            "gmd:characterSet/gmd:CharacterSetCode/@codeListValue",
        ],
        multiplicity="0..1",
    ),
    USGINISOElement(
        name="metadataCharacterSetCodeList",
        search_paths=[
            "gmd:characterSet/gmd:CharacterSetCode/@codeList",
        ],
        multiplicity="0..1",
    ),
    USGINISOElement(
        name="metadataParentIdentifier",
        search_paths=[
            "gmd:parentIdentifier/gco:CharacterString/text()",
        ],
        multiplicity="0..1",
    ),
    USGINISOElement(
        name="metadataHierarchyLevel",
        search_paths=[
            "gmd:hierarchyLevel/gmd:MD_ScopeCode/@codeListValue",
            "gmd:hierarchyLevel/gmd:MD_ScopeCode/text()",
        ],
        multiplicity="*",
    ),
          # smr add scopeCode codelist 
    USGINISOElement(
        name="metadataHierarchyLevelCodelist",
        search_paths=[
            "gmd:hierarchyLevel/gmd:MD_ScopeCode/@codeList",
        ],
        multiplicity="*",
    ),
   
    #hierarchyLevelName is used as resourceType by USGIN
    USGINISOElement(
        name="metadataHierarchyLevelName",
        search_paths=["gmd:hierarchyLevelName/gco:CharacterString/text()"],
        multiplicity="*",
    ),
            
    USGINISOElement(
        name="metadataStandardName",
        search_paths=["gmd:metadataStandardName/gco:CharacterString/text()"],
        multiplicity="0..1",
    ),
    USGINISOElement(
        name="metadataStandardVersion",
        search_paths=["gmd:metadataStandardVersion/gco:CharacterString/text()"],
        multiplicity="0..1",
    ),
    
    # correct the xpath for metadata point of contact, was mapped to identificationInfo POC which is the resource POC 
    ISOResponsibleParty(
        name="metadataContacts",
        search_paths=[
            "gmd:contact"
        ],
        multiplicity="1..*",
    ),
    
    #metadata time stamp
    USGINISOElement(
        name="metadataDate",
        search_paths=[
            "gmd:dateStamp/gco:DateTime/text()",
            "gmd:dateStamp/gco:Date/text()",
        ],
        multiplicity="1",
    )
]
    


# ## Spatial representation and reference system

# In[6]:


# don't process SpatialRepresentation; -1 multiplicity throws warning in the log
# TODO Need to build handlers for MD_GridSpatialRepresentation and MD_VectorSpatialRepresentation
elements.append(USGINISOElement(
    name="resourceSpatialRepresentation",
    search_paths=[
        "gmd:spatialRepresentationInfo",
    ],
    multiplicity="-1",
))

#spatial reference system
elements.append(USGINISOElement(
    name="resourceSpatialReferenceSystem",
    search_paths=[
        "gmd:referenceSystemInfo/gmd:MD_ReferenceSystem/gmd:referenceSystemIdentifier/gmd:RS_Identifier/gmd:code/gco:CharacterString/text()",
    ],
    multiplicity="*",
))
#smr add spatial reference system authority title 
elements.append(USGINISOElement(
    name="resourceSRSAuthorityName",
    search_paths=[
        "gmd:referenceSystemInfo/gmd:MD_ReferenceSystem/gmd:referenceSystemIdentifier/gmd:RS_Identifier/gmd:authority/gmd:CI_Citation/gmd:title/gco:CharacterString/text()",
    ],
    multiplicity="0..1",
))
#smr add spatial reference system identifier codespace 
elements.append(USGINISOElement(
    name="resourceSRSCodespace",
    search_paths=[
        "gmd:referenceSystemInfo/gmd:MD_ReferenceSystem/gmd:referenceSystemIdentifier/gmd:RS_Identifier/gmd:codeSpace/gco:CharacterString/text()",
    ],
    multiplicity="0..1",
))



# ## Identification information

# ### Citation

# In[7]:


###### identificationInfo CI_Citation section '       
       
# title
elements.append(USGINISOElement(
    name="resourceTitle",
    search_paths=[
        "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:citation/gmd:CI_Citation/gmd:title/gco:CharacterString/text()",
        "gmd:identificationInfo/srv:SV_ServiceIdentification/gmd:citation/gmd:CI_Citation/gmd:title/gco:CharacterString/text()",
    ],
    multiplicity="1",
))

# Authors
elements.append(ISOResponsibleParty(
    name="citationResponsibleParties",
    search_paths=[
        "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:citation/gmd:CI_Citation/gmd:citedResponsibleParty",
        "gmd:identificationInfo/srv:SV_ServiceIdentification/gmd:citation/gmd:CI_Citation/gmd:citedResponsibleParty"
    ],
    multiplicity="*"
))

#alternateTitle
elements.append(USGINISOElement(
    name="citationAlternateTitles",
    search_paths=[
        "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:citation/gmd:CI_Citation/gmd:alternateTitle/gco:CharacterString/text()",
        "gmd:identificationInfo/srv:SV_ServiceIdentification/gmd:citation/gmd:CI_Citation/gmd:alternateTitle/gco:CharacterString/text()",
    ],
    multiplicity="*",
))

#resource reference dates
elements.append(ISOReferenceDate(
    name="citationDates",
    search_paths=[
        "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:citation/gmd:CI_Citation/gmd:date/gmd:CI_Date",
        "gmd:identificationInfo/srv:SV_ServiceIdentification/gmd:citation/gmd:CI_Citation/gmd:date/gmd:CI_Date",
    ],
    multiplicity="1..*",
))

#resource identifier; reconcole with DataSetURI; inclued ISSN and ISBN here 
 # smr add datasetURI; have to reconcile with MD_Identifier in information//CI_Citation 
elements.append(USGINISOElement(
    name="resourceIdentifiers",
    search_paths=[
        "gmd:dataSetURI/gco:CharacterString/text()",
        "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:citation/gmd:CI_Citation/gmd:identifier/gmd:MD_Identifier/gmd:code/gco:CharacterString/text()",
        "gmd:identificationInfo/gmd:SV_ServiceIdentification/gmd:citation/gmd:CI_Citation/gmd:identifier/gmd:MD_Identifier/gmd:code/gco:CharacterString/text()",
        "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:citation/gmd:CI_Citation/gmd:ISBN/gco:CharacterString/text()",
        "gmd:identificationInfo/gmd:SV_ServiceIdentification/gmd:citation/gmd:CI_Citation/gmd:ISBN/gco:CharacterString/text()",
        "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:citation/gmd:CI_Citation/gmd:ISSN/gco:CharacterString/text()",
        "gmd:identificationInfo/gmd:SV_ServiceIdentification/gmd:citation/gmd:CI_Citation/gmd:ISSN/gco:CharacterString/text()",
    ],
    multiplicity="0..*",
))

# edition and editionDate are added in additional citation info

#presentation forms
elements.append(USGINISOElement(
    name="publicationPresentationForm",
    search_paths=[
        "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:citation/gmd:CI_Citation/gmd:presentationForm/gmd:CI_PresentationFormCode/text()",
        "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:citation/gmd:CI_Citation/gmd:presentationForm/gmd:CI_PresentationFormCode/@codeListValue",
        "gmd:identificationInfo/srv:SV_ServiceIdentification/gmd:citation/gmd:CI_Citation/gmd:presentationForm/gmd:CI_PresentationFormCode/text()",
        "gmd:identificationInfo/srv:SV_ServiceIdentification/gmd:citation/gmd:CI_Citation/gmd:presentationForm/gmd:CI_PresentationFormCode/@codeListValue",

    ],
    multiplicity="*",
))

# presentation form codelist 
elements.append(USGINISOElement(
    name="publicationPresentationFormCodelist",
    search_paths=[
        "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:citation/gmd:CI_Citation/gmd:presentationForm/gmd:CI_PresentationFormCode/@codeList",
        "gmd:identificationInfo/srv:SV_ServiceIdentification/gmd:citation/gmd:CI_Citation/gmd:presentationForm/gmd:CI_PresentationFormCode/@codeList",

    ],
    multiplicity="*",
))


# ### Abstract

# In[8]:


#abstract for resource
elements.append(USGINISOElement(
    name="resourceAbstract",
    search_paths=[
        "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:abstract/gco:CharacterString/text()",
        "gmd:identificationInfo/srv:SV_ServiceIdentification/gmd:abstract/gco:CharacterString/text()",
        "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:supplementalInformation/gco:CharacterString/text()",
        "gmd:identificationInfo/srv:SV_ServiceIdentification/gmd:supplementalInformation/gco:CharacterString/text()",
    ],
    multiplicity="1",
))



# ### Other Resource Details
# edition, edition date, series information, collective title, Resource purpose, status, credit, point of contact, maintenance

# In[9]:


# smr add edition, edition date, series information, collective title into other citation details This is where the get values processing would need to look at all the xpaths, and concatenate results 
elements.append(USGINISOElement(
    name="publicationDescription",
    search_paths=[
        "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:edition/gco:CharacterString/text()",
        "gmd:identificationInfo/srv:SV_ServiceIdentification/gmd:edition/gco:CharacterString/text()",
        "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:editionDate/gco:Date/text()",
        "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:editionDate/gco:DateTime/text()"
        "gmd:identificationInfo/gmd:SV_ServiceIdentification/gmd:editionDate/gco:Date/text()",
        "gmd:identificationInfo/gmd:SV_ServiceIdentification/gmd:editionDate/gco:DateTime/text()",
        "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:series/gmd:CI_Series/gmd:name/gco:CharacterString/text()",
        "gmd:identificationInfo/srv:SV_ServiceIdentification/gmd:series/gmd:CI_Series/gmd:name/gco:CharacterString/text()",
        "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:series/gmd:CI_Series/gmd:issueIdentification/gco:CharacterString/text()",
        "gmd:identificationInfo/srv:SV_ServiceIdentification/gmd:series/gmd:CI_Series/gmd:issueIdentification/gco:CharacterString/text()",
        "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:series/gmd:CI_Series/gmd:page/gco:CharacterString/text()",
        "gmd:identificationInfo/srv:SV_ServiceIdentification/gmd:series/gmd:CI_Series/gmd:page/gco:CharacterString/text()",
        "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:otherCitationDetails/gco:CharacterString/text()",
        "gmd:identificationInfo/srv:SV_ServiceIdentification/gmd:otherCitationDetails/gco:CharacterString/text()",
        "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:collectiveTitle/gco:CharacterString/text()",
        "gmd:identificationInfo/srv:SV_ServiceIdentification/gmd:collectiveTitle/gco:CharacterString/text()",               
    ],
    multiplicity="0..1",
))

#resource purpose
elements.append(USGINISOElement(
    name="resourcePurpose",
    search_paths=[
        "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:purpose/gco:CharacterString/text()",
        "gmd:identificationInfo/srv:SV_ServiceIdentification/gmd:purpose/gco:CharacterString/text()",
    ],
    multiplicity="0..1",
))

# resource credit smr add
elements.append(USGINISOElement(
    name="resourceCredit",
    search_paths=[
        "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:credit/gco:CharacterString/text()",
        "gmd:identificationInfo/srv:SV_ServiceIdentification/gmd:credit/gco:CharacterString/text()",
    ],
    multiplicity="*",
))

 #progress code = status
elements.append(USGINISOElement(
    name="resourceStatus",  #alt name is status, can a second name be added?
    search_paths=[
        "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:status/gmd:MD_ProgressCode/@codeListValue",
        "gmd:identificationInfo/srv:SV_ServiceIdentification/gmd:status/gmd:MD_ProgressCode/@codeListValue",
        "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:status/gmd:MD_ProgressCode/text()",
        "gmd:identificationInfo/srv:SV_ServiceIdentification/gmd:status/gmd:MD_ProgressCode/text()",
    ],
    multiplicity="*",
))

# smr add progressCode codelist 
elements.append(USGINISOElement(
    name="resourceStatusCodelist",
    search_paths=[
        "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:status/gmd:MD_ProgressCode/@codeList",
        "gmd:identificationInfo/srv:SV_ServiceIdentification/gmd:status/gmd:MD_ProgressCode/@codeList",
    ],
    multiplicity="*",
))

# Resource POC (current steward for the resource)
elements.append(ISOResponsibleParty(
    name="resourceContacts",
    search_paths=[
        "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:pointOfContact/gmd:CI_ResponsibleParty",
        "gmd:identificationInfo/srv:SV_ServiceIdentification/gmd:pointOfContact/gmd:CI_ResponsibleParty",
    ],
    multiplicity="1..*",
))

# REsource maintenance
elements.append(ISOMaintenance(
    name="resourceMaintenance", 
    search_paths=[
        "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:resourceMaintenance/gmd:MD_MaintenanceInformation",
        "gmd:identificationInfo/srv:SV_ServiceIdentification/gmd:resourceMaintenance/gmd:MD_MaintenanceInformation",
    ],
    multiplicity="*",

))




# ### Resources browse graphic, native format, keywords, usage, constraints

# In[10]:


#Browse graphic
elements.append(ISOBrowseGraphic(
    name="resourceBrowseGraphic",
    search_paths=[
        "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:graphicOverview/gmd:MD_BrowseGraphic",
        "gmd:identificationInfo/srv:SV_ServiceIdentification/gmd:graphicOverview/gmd:MD_BrowseGraphic",
        
    ],
    multiplicity="*",
))

# native resource format 
elements.append(ISODataFormat(
    name="nativeFormats",
    search_paths=[
        "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:resourceFormat/gmd:MD_Format",
        "gmd:identificationInfo/srv:SV_ServiceIdentification/gmd:resourceFormat/gmd:MD_Format",
        
    ],
    multiplicity="*",
))

#keywords
elements.append(ISOKeyword(
    name="resourceIndexTerms",
    search_paths=[
        "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:descriptiveKeywords/gmd:MD_Keywords",
        "gmd:identificationInfo/srv:SV_ServiceIdentification/gmd:descriptiveKeywords/gmd:MD_Keywords",
    ],
    multiplicity="*"
))

# usage
elements.append(ISOUsage(
    name="resourceSpecificUsage",
    search_paths=[
        "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:resourceSpecificUsage/gmd:MD_Usage",
        "gmd:identificationInfo/srv:SV_ServiceIdentification/gmd:resourceSpecificUsage/gmd:MD_Usage",
    ],
    multiplicity="*"
))

#### constraints section
elements.append(ISOConstraints(
    name="resourceUsageConstraints",
    search_paths=[
        "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:resourceConstraints",
        
        "gmd:identificationInfo/srv:SV_ServiceIdentification/gmd:resourceConstraints",  
    ]
))


# ### Related resources, service type, resource spatial representation and resolution

# In[11]:


#use to link to other resources. Aggregation info
elements.append(ISOAggregationInfo(
    name="relatedResources",
    search_paths=[
        "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:aggregationInfo/gmd:MD_AggregateInformation",
        "gmd:identificationInfo/gmd:SV_ServiceIdentification/gmd:aggregationInfo/gmd:MD_AggregateInformation",
    ],
    multiplicity="*"
))

#service type  from service identification
elements.append(USGINISOElement(
    name="serviceType",
    search_paths=[
        "gmd:identificationInfo/srv:SV_ServiceIdentification/srv:serviceType/gco:LocalName/text()",
    ],
    multiplicity="0..1",
))


# Data identificiation specific elements
# spatial representation type code
elements.append(USGINISOElement(
    name="resourceSpatialRepresentationTypeURI",
    search_paths=[
        "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:spatialRepresentationType/gmd:MD_SpatialRepresentationTypeCode/@codeListValue",
        "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:spatialRepresentationType/gmd:MD_SpatialRepresentationTypeCode/text()",

    ],
    multiplicity="*",
))

elements.append(USGINISOElement(
    name="resourceSpatialRepresentationVocabularURI",
    search_paths=[
        "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:spatialRepresentationType/gmd:MD_SpatialRepresentationTypeCode/@codeList",
    ],
    multiplicity="*",
))

#spatial resolution, as distance or scale denominator
elements.append(USGINISOElement(
    name="resolutionDistanceValue",
    search_paths=[
        "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:spatialResolution/gmd:MD_Resolution/gmd:distance/gco:Distance/text()",    
    ],
    multiplicity="*",
))

elements.append(USGINISOElement(
    name="resolutionUOM",
    search_paths=[
        "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:spatialResolution/gmd:MD_Resolution/gmd:distance/gco:Distance/@uom",
        
    ],
    multiplicity="*",
))

elements.append(USGINISOElement(
    name="resolutionScaleDenominator",
    search_paths=[
        "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:spatialResolution/gmd:MD_Resolution/gmd:equivalentScale/gmd:MD_RepresentativeFraction/gmd:denominator/gco:Integer/text()",
        
    ],
    multiplicity="*",
))
    


# ### Resource language, topic category, environment description

# In[12]:


#resource language
elements.append(USGINISOElement(
    name="resourceLanguages",
    search_paths=[
        "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:language/gmd:LanguageCode/@codeListValue",
  
        "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:language/gmd:LanguageCode/text()",
        "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:language/gmd:CharacterString/text()",

    ],
    multiplicity="*",
))

#topic category
elements.append(USGINISOElement(
    name="topicCategory",
    search_paths=[
        "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:topicCategory/gmd:MD_TopicCategoryCode/text()",   
    ],
    multiplicity="*",
))

#smr add environmentDescription in case someone uses that for software environment...
elements.append(USGINISOElement(
    name="resourceEnvironmentDescription",
    search_paths=[
        "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:environmentDescription/gco:CharacterString/text()",
    ],
    multiplicity="0..1",
))



# ### Spatial Extent

# In[13]:


#extent, controlled-- move the geographicIdentifier values from extent-free-text to here...
#  don't have a handler for EX_BoundingPolygon... should throw warning if have one
elements.append(USGINISOElement(
    name="extentReference",
    search_paths=[
        "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:extent/gmd:EX_Extent/gmd:geographicElement/gmd:EX_GeographicDescription/gmd:geographicIdentifier/gmd:MD_Identifier/gmd:code/gco:CharacterString/text()",
        "gmd:identificationInfo/srv:SV_ServiceIdentification/srv:extent/gmd:EX_Extent/gmd:geographicElement/gmd:EX_GeographicDescription/gmd:geographicIdentifier/gmd:MD_Identifier/gmd:code/gco:CharacterString/text()",
    ],
    multiplicity="*",
))

# smr put in xpath for EX_extent/description.
elements.append(USGINISOElement(
    name="extentStatement",
    search_paths=[
        "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:extent/gmd:EX_Extent/gmd:description/gco:CharacterString/text()",
        "gmd:identificationInfo/srv:SV_ServiceIdentification/srv:extent/gmd:EX_Extent/gmd:description/gco:CharacterString/text()",
    ],
    multiplicity="*",
))

elements.append(ISOBoundingBox(
    name="boundingBoxesWGS84",
    search_paths=[
        "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:extent/gmd:EX_Extent/gmd:geographicElement/gmd:EX_GeographicBoundingBox",
        "gmd:identificationInfo/srv:SV_ServiceIdentification/srv:extent/gmd:EX_Extent/gmd:geographicElement/gmd:EX_GeographicBoundingBox",
    ],
    multiplicity="*",
))




# ### Temporal extent

# In[14]:


#smr add time instant, for gml ns only; if is instant make extent-begin=extent-end..
#also that gml3.2 should be invalid with gmd...
elements.append(USGINISOElement(
    name="timePeriodBeginPosition",
    search_paths=[
        "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:extent/gmd:EX_Extent/gmd:temporalElement/gmd:EX_TemporalExtent/gmd:extent/gml:TimePeriod/gml:beginPosition/text()",
        "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:extent/gmd:EX_Extent/gmd:temporalElement/gmd:EX_TemporalExtent/gmd:extent/gml32:TimePeriod/gml32:beginPosition/text()",
        "gmd:identificationInfo/srv:SV_ServiceIdentification/srv:extent/gmd:EX_Extent/gmd:temporalElement/gmd:EX_TemporalExtent/gmd:extent/gml:TimePeriod/gml:beginPosition/text()",
        "gmd:identificationInfo/srv:SV_ServiceIdentification/srv:extent/gmd:EX_Extent/gmd:temporalElement/gmd:EX_TemporalExtent/gmd:extent/gml32:TimePeriod/gml32:beginPosition/text()",
        "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:extent/gmd:EX_Extent/gmd:temporalElement/gmd:EX_TemporalExtent/gmd:extent/gml:TimeInstant/gml:timePosition/text()",
        "gmd:identificationInfo/srv:SV_ServiceIdentification/srv:extent/gmd:EX_Extent/gmd:temporalElement/gmd:EX_TemporalExtent/gmd:extent/gml:TimeInstant/gml:timePosition/text()",
    ],
    multiplicity="*",
))

elements.append(USGINISOElement(
    name="timePeriodEndPosition",
    search_paths=[
        "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:extent/gmd:EX_Extent/gmd:temporalElement/gmd:EX_TemporalExtent/gmd:extent/gml:TimePeriod/gml:endPosition/text()",
        "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:extent/gmd:EX_Extent/gmd:temporalElement/gmd:EX_TemporalExtent/gmd:extent/gml32:TimePeriod/gml32:endPosition/text()",
        "gmd:identificationInfo/srv:SV_ServiceIdentification/srv:extent/gmd:EX_Extent/gmd:temporalElement/gmd:EX_TemporalExtent/gmd:extent/gml:TimePeriod/gml:endPosition/text()",
        "gmd:identificationInfo/srv:SV_ServiceIdentification/srv:extent/gmd:EX_Extent/gmd:temporalElement/gmd:EX_TemporalExtent/gmd:extent/gml32:TimePeriod/gml32:endPosition/text()",
        "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:extent/gmd:EX_Extent/gmd:temporalElement/gmd:EX_TemporalExtent/gmd:extent/gml:TimeInstant/gml:timePosition/text()",
        "gmd:identificationInfo/srv:SV_ServiceIdentification/srv:extent/gmd:EX_Extent/gmd:temporalElement/gmd:EX_TemporalExtent/gmd:extent/gml:TimeInstant/gml:timePosition/text()",
    ],
    multiplicity="*",
))

#vertical extent has minimumValue and maximumValue properties. Have to check what this search_path actually does.
elements.append(USGINISOElement(
    name="verticalExtent",
    search_paths=[
        "gmd:identificationInfo/gmd:MD_DataIdentification/gmd:extent/gmd:EX_Extent/gmd:verticalElement/gmd:EX_VerticalExtent",
        "gmd:identificationInfo/srv:SV_ServiceIdentification/srv:extent/gmd:EX_Extent/gmd:verticalElement/gmd:EX_VerticalExtent",
    ],
    multiplicity="*",
))



# In[15]:


#supplemental information on the dataset resource goes in the abstract



# ### service operation

# In[16]:


#target of operatesOn is gmd:MD_DataIdentification; smr change name to avoid confusion with real coupled-resource
elements.append(ISOCoupledResources(
    name="serviceOperatesOnReferences",
    search_paths=[
        "gmd:identificationInfo/srv:SV_ServiceIdentification/srv:operatesOn",
    ],
    multiplicity="*",
))

#smr add
# The apiso implementation of coupledResource only allows and operationName, an identifier (characterString), a scoped Name with codeSpace; note ScopedName breaks Entity/property capitalizaiton pattern
elements.append(USGINISOElement(
    name="serviceOperatesOn",
    search_paths=[
        "gmd:identificationInfo/srv:SV_ServiceIdentification/srv:coupledResource/srv:SV_CoupledResource",
    ],
    multiplicity="*",
    elements = [
        ISOCoupledResources(
            name="serviceOperatesOnResourceID",
            search_paths=[
                "srv:identifier/gco:CharacterString/text()"
            ],
            multiplicity="1",
        ),
        USGINISOElement(
            name="serviceOperatesOnScopedName",
            search_paths=[
                "gco:ScopedName/text()"
            ],
            multiplicity="0..1",
        ),
        USGINISOElement(
            name="serviceOperationName",
            search_paths=[
                "srv:operationName/gco:CharacterString/text()"
            ],
            multiplicity="0..1"
        ),           
    ],
))






# ## Distribution section

# In[17]:


############# Distribution section. logic will be necessary to map distribution consistently to the metadata JSON

#smr add, access options, grouped by distributor. including linked format, digital transfer otpions and standard order process.  required if multiple distributors are present 
elements.append(ISODistributorAccessOptions(
    name="distributorAccessOptions",
    search_paths=[
        "gmd:distributionInfo/gmd:MD_Distribution/gmd:distributor/gmd:MD_Distributor"
    ],
    multiplicity="*",
))
      

# from MD_Distribution    Handler for formats at the distribution level
# have to merge with distributorFormat; DistributionFormat goes with first
# distributor if there is more than one distributor

elements.append(ISOTransferOptions(
    name="distributionTransferOptions",
    search_paths=[
        "gmd:distributionInfo/gmd:MD_Distribution/gmd:transferOptions/gmd:MD_DigitalTransferOptions",
    ],
    multiplicity="*",
))

#smr add
elements.append(ISODataFormat(
    name="distributionFormat",
    search_paths=[
        "gmd:distributionInfo/gmd:MD_Distribution/gmd:distributionFormat/gmd:MD_Format",
    ],
    multiplicity="*",
))

#distributor contacts, only useful if there is only one distributor
elements.append(ISOResponsibleParty(
    name="distributorContact",
    search_paths=[
        "gmd:distributionInfo/gmd:MD_Distribution/gmd:distributor/gmd:MD_Distributor/gmd:distributorContact",
    ],
    multiplicity="*",
))




# ## Data Quality

# In[18]:


#the handling of data quality here is complelely in inadequate for the possible complexity of DQ_DataQuality. Fortunatey it almost never shows up in metadata...
#grab the specifications used for conformance results

# Quality <<<  Note that CKAN ISODocument object pulls explanation from gmd:DQ_DomainConsistency
#   into conformity-explanation. Handler for quality needs to be a complex object like ResponsibleParty
#   Include in this array of paths the paths for DQ_Elements that seem likely to have text explanations... [SMR 2014-03-21]
# SMR 2015-10-14; add this section
elements.append(USGINISOElement(
    name="qualityResultExplanations",
    search_paths=[
        "/gmd:dataQualityInfo/gmd:DQ_DataQuality/gmd:report/gmd:DQ_CompletenessCommission/gmd:result/gmd:DQ_ConformanceResult/gmd:explanation/gco:CharacterString/text()",
        "/gmd:dataQualityInfo/gmd:DQ_DataQuality/gmd:report/gmd:DQ_CompletenessOmission/gmd:result/gmd:DQ_ConformanceResult/gmd:explanation/gco:CharacterString/text()",
        "/gmd:dataQualityInfo/gmd:DQ_DataQuality/gmd:report/gmd:DQ_ConceptualConsistency/gmd:result/gmd:DQ_ConformanceResult/gmd:explanation/gco:CharacterString/text()",
        "/gmd:dataQualityInfo/gmd:DQ_DataQuality/gmd:report/gmd:DQ_FormatConsistency/gmd:result/gmd:DQ_ConformanceResult/gmd:explanation/gco:CharacterString/text()",
        "/gmd:dataQualityInfo/gmd:DQ_DataQuality/gmd:report/gmd:DQ_TopologicalConsistency/gmd:result/gmd:DQ_ConformanceResult/gmd:explanation/gco:CharacterString/text()",
        "/gmd:dataQualityInfo/gmd:DQ_DataQuality/gmd:report/gmd:DQ_NonQuantitativeAttributeAccuracy/gmd:result/gmd:DQ_ConformanceResult/gmd:explanation/gco:CharacterString/text()",
        "/gmd:dataQualityInfo/gmd:DQ_DataQuality/gmd:report/gmd:DQ_ThematicClassificationCorrectness/gmd:result/gmd:DQ_ConformanceResult/gmd:explanation/gco:CharacterString/text()",
        "/gmd:dataQualityInfo/gmd:DQ_DataQuality/gmd:report/gmd:DQ_TemporalConsistency/gmd:result/gmd:DQ_ConformanceResult/gmd:explanation/gco:CharacterString/text()",
        "/gmd:dataQualityInfo/gmd:DQ_DataQuality/gmd:report/gmd:DQ_TemporalValidity/gmd:result/gmd:DQ_ConformanceResult/gmd:explanation/gco:CharacterString/text()"
    ],
    multiplicity="*", # "*", "1..*", "1" are other options
))

### experiment to see if this works...
#grab the conformance result pass values. 
elements.append(USGINISOElement(
    name="qualityResultPass",
    search_paths=[
        "gmd:dataQualityInfo/gmd:DQ_DataQuality/gmd:report//gmd:result/gmd:DQ_ConformanceResult/gmd:pass/gco:Boolean/text()",
    ],
    multiplicity="*",
))

#quality conformity explanation
elements.append(USGINISOElement(
    name="qualityResultSpecifications",
    search_paths=[
        "gmd:dataQualityInfo/gmd:DQ_DataQuality/gmd:report//gmd:result/gmd:DQ_ConformanceResult/gmd:explanation/gco:CharacterString/text()",
    ],
    multiplicity="*",
))

elements.append(USGINISOElement(
    name="unhandledQualityReportResults",
    search_paths=[
        "gmd:dataQualityInfo/gmd:DQ_DataQuality/gmd:report/gmd:DQ_DomainConsistency/gmd:result/gmd:DQ_QuantitativeResult",
        "gmd:dataQualityInfo/gmd:DQ_DataQuality/gmd:report/gmd:DQ_DomainConsistency/gmd:result/gmd:DQ_CoverageResult",
    ],
    multiplicity="-1",
))

 



# ## Lineage

# In[19]:


#lineage statement
elements.append(USGINISOElement(
    name="resourceLineageItems",
    search_paths=[
        "gmd:dataQualityInfo/gmd:DQ_DataQuality/gmd:lineage/gmd:LI_Lineage/gmd:statement/gco:CharacterString/text()",
    ],
    multiplicity="*",
))

#test to catch if there are any other lineage element to log the fact that they aren't processed
# the -1 multiplicity will put warning in the log.
elements.append(USGINISOElement(
    name="lineage-not-processed",
    search_paths=[
        "gmd:dataQualityInfo/gmd:DQ_DataQuality/gmd:lineage/gmd:LI_Lineage/gmd:source",
        "gmd:dataQualityInfo/gmd:DQ_DataQuality/gmd:lineage/gmd:LI_Lineage/gmd:processStep",
    ],
    multiplicity="-1"
))




# ## Metadata constraints and maintenance

# In[20]:


#metadataConstraints
elements.append(ISOConstraints(
    name="metadataUsageConstraint",
    search_paths=[
        "gmd:metadataConstraints", 
    ],
multiplicity="*"
))

# Metadata maintenance
elements.append(ISOMaintenance(
    name="metadataMaintenance", 
    search_paths=[
        "gmd:metadataMaintenance/gmd:MD_MaintenanceInformation",
    ],
    multiplicity="0..1",

))
      


# ## Not processed
# Applicationi schema, portrayal catalog, content information

# In[21]:


#set flags for elements that we don't have parsers for yet...
elements.append(USGINISOElement(
    name="applicationSchema-not-processed",
    search_paths=[
        "gmd:applicationSchemaInfo",
    ],
    multiplicity="-1"
))

elements.append(USGINISOElement(
    name="portrayalCatalogue-not-processed",
    search_paths=[
        "gmd:portrayalCatalogueInfo",
    ],
    multiplicity="-1"
))

elements.append(USGINISOElement(
    name="contentInfo-not-processed",
    search_paths=[
        "gmd:contentInfo",
    ],
    multiplicity="-1"
))


# ## Helper functions for ISO mapping

# In[22]:


def infer_values(self, values):
    # Todo: Infer name.
    self.infer_date_released(values)
    self.infer_date_updated(values)
    self.infer_date_created(values)
    self.infer_url(values)
    # Todo: Infer resources.
    self.infer_tags(values)
    self.infer_publisher(values)
    self.infer_contact(values)
    self.infer_contact_email(values)
    return values

def infer_date_released(self, values):
    value = ''
    for date in values['dataset-reference-date']:
        if date['type'] == 'publication':
            value = date['value']
            break
    values['date-released'] = value
    # from usgin.py 
    if value:
        date_obj = parse(value)
        value = date_obj.replace(tzinfo=None)
    values['publication_date'] = value #add to account for NgdsXmlMapping

def infer_date_updated(self, values):
    value = ''
    dates = []
    # Use last of several multiple revision dates.
    for date in values['dataset-reference-date']:
        if date['type'] == 'revision':
            dates.append(date['value'])

    if len(dates):
        if len(dates) > 1:
            dates.sort(reverse=True)
        value = dates[0]

    values['date-updated'] = value

def infer_date_created(self, values):
    value = ''
    for date in values['dataset-reference-date']:
        if date['type'] == 'creation':
            value = date['value']
            break
    values['date-created'] = value

def infer_url(self, values):
    value = ''
    for locator in values['resource-locator']:
        if locator['function'] == 'information':
            value = locator['url']
            break
    values['url'] = value

def infer_tags(self, values):
    tags = []
    for key in ['keyword-inspire-theme', 'keyword-controlled-other']:
        for item in values[key]:
            if item not in tags:
                tags.append(item)
    values['tags'] = tags

def infer_publisher(self, values):
    value = ''
    for responsible_party in values['responsible-organisation']:
        if responsible_party['role'] == 'publisher':
            value = responsible_party['organisation-name']
        if value:
            break
    values['publisher'] = value

def infer_contact(self, values):
    value = ''
    for responsible_party in values['responsible-organisation']:
        value = responsible_party['organisation-name']
        if value:
            break
    values['contact'] = value

def infer_contact_email(self, values):
    value = ''
    for responsible_party in values['responsible-organisation']:
        if isinstance(responsible_party, dict) and            isinstance(responsible_party.get('contact-info'), dict) and            responsible_party['contact-info'].has_key('email'):
            value = responsible_party['contact-info']['email']
            if value:
                break
    values['contact-email'] = value


# # Build USGIN CKAN package: USGINHarvester class

# ## Package dictionary component assemblers

# In[23]:


# %load E:\GitHub\NGDS\ckanext-metadata-bku03232018\ckanext\harvest\usgin.py


#CSWHarvester extends SpatialHarvester, which appears to implement ISpatialHarvester
#  but does not explicitly declare that it does.
#CSWHarvester is defined in ckanext-spatial/ckanest/spatial/harversters/csw.py

class USGINHarvester(imp.CSWHarvester):

    force_import = False

    def info(self):
        """Return some information about this particular harvester"""
        return {
            'name': 'USGIN-harvester',
            'title': 'USGIN CSW harvester',
            'description': 'Class for processing USGINXmlMapping into a JSON object'
        }

    
    def contact(self, data):
        return {
            "contactRef": {
                "agentRole": {
                    "conceptPrefLabel":  data.get("role", None),
                    "vocabularyURI":  data.get("role-codespace", None),
                    "contactAddress": None,
                    "contactEmail": data.get("contact-info", None).get("email", None),
                    "individual": {
                        "personName": data.get("individual-name", None),
                        "personPosition": data.get("position-name", None),
                        "personRole": data.get("role", None),
                    },
                    "organizationName": data.get("organisation-name", None)
                }
            }
        }
    # assume voice telephone
    def buildTelephone(self,data):
        return {
            "phoneNumber": data,
            "phoneLabel":"voice"
        }

    #smr responsible party-- an agent in a role
    def buildContactInRole(self, data):
        # data is ISOResponsibleParty class from harvested_metadata.py
        
        postaladdress=data.get("contact-info", None).get("postal-address", None)
        thecontactaddress = ('"' + 
            postaladdress.get("delivery-point", None) + ', ' + 
            postaladdress.get("city", None) + ', ' + 
            postaladdress.get("administrative-area", None) + ', ' + 
            postaladdress.get("postal-code", None) + ', ' + 
            postaladdress.get("country", None) + '"')
        # concatenate all the fields into one address string.
        return  {
        "agentRole":{
        "conceptURI":data.get("agentRoleConceptURI", None),
        "conceptPrefLabel":data.get("agentRolePrefLabel", None),
        "vocabularyURI":data.get("agentRoleVocabularyURI", None)
        },
        "agent": {
            "personName": data.get("contact", None).data.get("personName", None),
            "personPosition": data.get("contact", None).data.get("personPosition", None),
            "organizationNames": [data.get("contact", None).data.get("organizationNames", None)],
            # if data.get("contact-info", None).get("telephone-voice", []): 
            "phoneContacts":[ self.buildTelephone(thetel) for thetel in data.get("contact", None).data.get("contact-info", None).get("telephone-voice", [])
                ],
            "contactEmails": [data.get("contact", None).data.get("contact-info", None).get("contactEmails", None)],
            "contactAddress": thecontactaddress,   
            "organizationLinks": [data.get("contact", None).data.get("contact-info", None).data.get("contact-link", None)]
              }
        }

    def buildBboxes(self, data):
        #modify to return multiple bboxes if they exist. target to populate boundingBoxesWGS84
        # in USGIN JSON metadata
        bboxes = []
        for bbox in data.get("bbox", None):
            if bbox:
                bboxes = bboxes.append({
                "eastBoundLongitude": bbox.get("east", ""),
                "northBoundLatitude": bbox.get("north", ""),
                "southBoundLatitude": bbox.get("south", ""),
                "westBoundLongitude": bbox.get("west", "")
            })
        return bboxes

    def buildAccessLink(self, data):
        protocol = data.get("protocol", None)

        if protocol is None:
            protocol = data.get("resource_locator_protocol", None)

        description = data.get("description", None)
        ogc_layer = None
        link_description = None

        if description and protocol.lower() == 'ogc:wms':
            regex = re.match('parameters:{layers:"(.*)"}', description)

            try:
                layer = regex.group(1) if regex else None
            except:
                layer = None

            ogc_layer = layer
            link_description = None
            
            #toDo-- need to pull parameters out of description string and put in the linkObject.linkParameters array

        if description and protocol.lower() == 'ogc:wfs':
            regex = re.match('parameters:{typeName:"(.*)"}', description)

            try:
                layer = regex.group(1) if regex else None
            except:
                layer = None

            ogc_layer = layer
            link_description = None

        if description and protocol.lower() not in ['ogc:wfs', 'ogc:wms']:
            link_description = description

        link_obj = {
            "linkObject": {
                "url": data.get("url", None),
                "linkTitle": data.get("name", None),
                "linkTargetResourceType": protocol,
                "linkContentResourceType": protocol,
                "description": link_description,
                "ogc_layer": ogc_layer
            }
        }
        return link_obj

    def buildDistributions(self, data):
        """
        Each distributor may have multiple digitalTrasferOptions, MD_format, standard order process. 
        if only one distributor is present, links from MD_Distribution to transferOptions and format have to be projected to distributor
        if no distributor provided, create an unknown and associate with the required DigitalTransferOptions linksage
        These distributions correspond to ckan package.resources
        """
        pass  #TODO 

    def buildContentInfo(self, data):
        """
    scan keywords for usgincm: prefix and pull content model name to gmd:contentInfo/gmd:MD_FeatureCatalogDescription/
    gmd:featureTypes genericName, and put link for schema.usgin.org/models/ in the featureCatalogCitation.
    Should probably implement mapping for gmd:contentInfo part of metadata as well...
    Model version goes in citation edition, model URI (namespace URI) goes in citation identifier
        """
        pass #ToDo

    def buildResourceType(self, data):
        """
    put hierachyLevel/scopeCode, hierachrylevelName, and keywords with usginres: prefix in here as resource types, with appropriate citation tiles for the sources.
        """
        pass #ToDo


# ## Main package dictionary assembly

# In[24]:


def get_usginpackage_dict(self, iso_values, isoxml):
    """
    This function gets the package dict from spatial/harvester/base.py, and 
    used the USGIN ISO mapping (in usgin_xml_mapper) to construct extras.usginmdpackage
    with USGIN metadata content.
    
    
    :param iso_values: Dictionary with parsed values from the ISO 19139
        XML document
    :type iso_values: dict
    :param isoxml: ISO19139 xml metadata record as a string
    :type isoxml: string

    :returns: A dataset dictionary (package_dict)
    :rtype: dict
    """

    # First generate exactly the same package dict that the standard harvester would.
    # this executes get_package_dict on CSWHarvester, which is the parent of USGINHarvester
    # CSWHarverster inherits get_package_dict from SpatialHarvester, defined in 
    # ckanext-spatial/ ckanext/spatial/harversters/base.py. 
    # the idea is that everything outside of the extra.usginmd_package is the same as out of
    # the box spatial harvester, so it doesn't break other stuff.
    package_dict = super(USGINHarvester, self).get_package_dict(iso_values, harvest_object)

    # Then lets parse the harvested XML document with a customized NGDS parser; this function
    # is defined in ckanext-metadata/ckanext/usginxml_reader.py
    doc = USGINXmlMapping(xml_str=isoxml)
    values = doc.read_values()

    # pull extras from package_dict created with base.py (harvested_metadata.py)
    extras = package_dict['extras']

    # Published or unpublished
    package_dict['private'] = False



    cited_source_agent = [self.buildRelatedAgent(agent) for agent in values.get('citationResponsibleParties', [])]
    metadata_contact = [self.buildRelatedAgent(agent) for agent in values.get('metadataPointOfContact', [])]
    resource_contact = [self.buildRelatedAgent(agent) for agent in values.get('resource-contact', [])]
    
    #build new handler for distributions; distributors array will have the transferOptions (access_links) and formats 
    # inside, along with distributor contact information
    # distributors = [self.buildRelatedAgent(agent) for agent in values.get('distributor', [])]
    # access_links = [self.buildAccessLink(res) for res in values.get('resource-locator', [])]
  
    # TODO  construct a distributor access options object to put in md_package.  Group on distributors, with a collection of online or offline distributions options associated with each distributor
    
    # this md_package is constructed against USGINMetadataJSONv3.0, 
    # the value dictionary is constructed in usgin_xml_reader.py, should be in the same 
    # directory as this file

    datelist = values.get("dataset-reference-date", "")
    usgin_md_package = {
        "metadataProperties" : {
            "metadataIdentifier" : values.get("metadataIdentifier", ""),
            "metadataLastUpdate" : values.get("metadataDate", ""),
            "metadataContacts" : metadata_contact,
            "metadataSpecification" : {
                "referenceLabel" : "USGIN JSON metadata v3.0",
                "referenceURI" : "http://resources.usgin.org/uri-gin/usgin/schema/json/3.0/metadata"
            },
            "parentMetadata" : {
                "referenceURI" : values.get("metadataParentIdentifier", "")
            },
            "metadataMaintenance" : values.get("metadataMaintenance", ""),

            "metadataLanguage" : {
                "languageCode" : values.get("metadataLanguageCode", ""),
                "languageReference" : {
                    "referenceURI" : values.get("metadataLanguageCodeList", ""),

                }
            },

            # this will be filled from metadataRecordLineageItems;
            # but content is sparse at this point
            
            "metadataRecordLineageItems" : [],
            "metadataUsageConstraint" : [values.get("metadataUsageConstraint","")], 
            "harvestInformation" : {
                "harvestDate" : "",
                "harvestedFileIdentifier" : values.get("metadataIdentifiervalues",""),
                "harvestedMetadataFormat" : {
                    "referenceLabel" : values.get("metadataStandardName", ""),
                    "version" : values.get("metadataStandardVersion", "")
                },
                "harvestURL" : "",
            },
        },
        
            # have to handle these...
            # "usginContentModel" : "",
            # "usginContentModelLayer" : "",
            # "usginContentModelVersion" : "",
        "resourceDescription" : {
            "resourceTitle" : values.get("title", ""),
            "resourceAbstract" : values.get("abstract", ""),
            "citationResponsibleParties" : cited_source_agent,
            "citationDates" : {
                "EventDateObject" : {
                    "dateTime" : datelist[0].get("value", "")
                }
            },
            "citationAlternateTitles":"",
            "citationRecommendation":"",
            "citationDetails":"",
            "resourceTypes":[],
            "resourceStatus":"",
            "resourceContacts" : resource_contact,
            "resourceBrowseGraphic":"",
            "resourceTemporalExtents":"",
            "resourceCharacterSet":"",
            "resourceLanguages":[],
            "resourceSpatialExtents":[],
            "resourceSpatialDescription":{},
            "resourceIndexTerms":[],
            "resourceAccessOptions" : [],
            "resourceLineageItems":[],
            "resourceQualityItems":[],
            "resourceUsageConstraints":[],
            "resourceMaintenance":{},
            "resourcePurpose":"",
            "resourceCredit":"",
            "topicCategory":[],
            "resourceEnvironmentDescription":"",
            "resourceSpecificUsage":[],
            "relatedResources":[],
            "resourceDetails":{
                "dataset":{},
                "service":{}
            }
        },
        }
    usgin_md_package = json.dumps(usgin_md_package)

    extras.append({"key": "md_package", "value": usgin_md_package})

    # When finished, be sure to return the dict
    return package_dict


# # Code for testing

# In[25]:


def testurl(theurl):
    #try HEAD first in case the response document is big

    try:
        #print('test url %s' % theurl)
        try:  #in case endpoint doesn't implement head
            r = requests.head(theurl)
        except:
            pass
        #print('after request head')
        if (r is not None and 
            r.status_code != requests.codes.ok):
            #check GET in case is an incomplete http implementation
            r = requests.get(theurl)
            #print('after request get')
            if (r.status_code == requests.codes.ok):
                return True
            else:
                #print('get status code not OK')
                return False
        else:
            return True
    except:
        return False


# In[26]:


# use hardwired values for testing
#catalogURL = "http://cinergi.sdsc.edu/geoportal/"  #geoportal v 2.5
documentID=''
catalogURL = "http://catalog.usgin.org/geoportal/"  #geoportal v1.2
if (len(documentID)==0):
    documentID='%7B40DC5A1A-7962-4BD0-BB96-ACF905C2DA8B%7D'
    #documentID='%7B1E098C44-8378-41BC-9918-1627EC4F654D%7D'
    
# get xml record
#get the url to retrieve xml record from ESRI geoportal catalog
metadataURLx=catalogURL + 'rest/document?id=' + documentID 



#get the xml record
if testurl(metadataURLx):
    the_page = requests.get(metadataURLx)
    print ('metadata URL: %s' % metadataURLx)
else:
    the_page=None
    print('url %s failed' % metadataURLx)


# In[ ]:



# Parse ISO document
try:
    #iso_parser = ISODocument(harvest_object.content)
    iso_parser = imp.ISODocument(the_page.text)
    iso_values = iso_parser.read_values()
except Exception, e:
    print('Error parsing ISO document for %s' % metadataURLx)

print(json.dumps(iso_values))

thedict=USGINHarvester.get_usginpackage_dict(iso_values, the_page.text)
print(thedict)


# In[ ]:


print(metadataURLx)

