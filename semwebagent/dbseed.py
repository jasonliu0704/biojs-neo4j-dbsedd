import sparql
from neo4j.v1 import GraphDatabase, basic_auth
from urlparse import urlparse

driver = GraphDatabase.driver("bolt://localhost", auth=basic_auth("neo4j", "52@USA0704"))

session = driver.session()

# sparql query templates
get_types_query_count = "SELECT (count(distinct ?type) as ?count) WHERE {?s a ?type . FILTER (!isBlank(?s)) }"
get_types_query = "SELECT distinct ?type WHERE {?s a ?type . FILTER (!isBlank(?s))}"
get_predicates = "SELECT distinct ?predicate WHERE {?s a <@id> ; ?predicate ?o  . FILTER (?predicate != <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>)    }"
get_related_types = "SELECT distinct ?relatedType WHERE {?s a <@type> . ?s <@pred> [ a ?relatedType] . }"
get_related_types_count = "SELECT (count(*) as ?count) WHERE {?s a <@type> . ?s <@pred> [ a <@relatedType>] . }"

check_subtype = "ASK { <@type> <http://www.w3.org/2000/01/rdf-schema#subClassOf>* <@inputtype>}"
# sparql endpoint
endpoint = "http://www.ebi.ac.uk/rdf/services/ensembl/sparql"


print 'start'

result = sparql.query(endpoint, get_types_query_count)

print 'finishing querying type count'
count = 0


for row in result:
    values = sparql.unpack_row(row)
    count = values[0]

print 'number of types '+str(count)

types = []


def createCypher(nodeUri, nodeLabel, predicateUri, predicateLabel, objectUri, objectLabel, count, isLiteral):        
    result = ''

    if isLiteral:
        query = "MERGE (n:Uri {uri: {subject}, label : {subjectLabel} })"
        session.run(query, {"subject":nodeUri, "subjectLabel" : nodeLabel})
        query = "MATCH (n:Uri {uri: {subject} }) MERGE (n)-[p:Related {uri: {predicate}, label:{predicateLabel} }]->(n2:Literal {id: {literal} })"
        result = session.run(query, {"subject":nodeUri, "predicate": predicateUri, "literal" : nodeUri+predicateUri, "predicateLabel": predicateLabel})

    else:
        query = "MERGE (n:Uri {uri: {subject}, label : {subjectLabel} }) MERGE (n2:Uri {uri : {object}, label : {objectLabel} })"
        session.run(query, {"subject":nodeUri, "object" : objectUri, "subjectLabel" : nodeLabel, "objectLabel" : objectLabel })
        query = "MATCH (n:Uri {uri: {subject} }),(n2:Uri {uri : {object} }) MERGE (n)-[p:Related { uri: {predicate} , label: {predicateLabel}, count: {count} }]->(n2)"
        result = session.run(query, {"subject":nodeUri, "predicate": predicateUri, "object" : objectUri, "count": count, "predicateLabel" : predicateLabel})

    for record in result:
        print(", ".join("%s: %s" % (key, record[key]) for key in record.keys()))


def getLabel (uri):

    labelQuery = "SELECT ?label WHERE { <"+uri+"> <http://www.w3.org/2000/01/rdf-schema#label> ?label} limit 1"
    try:
        result = sparql.query(endpoint, labelQuery)
        for row in result:
            return  sparql.unpack_row(row)[0]
    except Exception:
        print Exception 
        print "query labels \n" 

    label = str(urlparse(uri).fragment)
    if not label:
        label = uri.rsplit('/', 1)[-1]
        if not label:
            return uri

    return label



for x in range(0, count, 100):
    try:
        result = sparql.query(endpoint, get_types_query + ' ORDER BY ?type LIMIT 10 OFFSET ' + str(x))
    except Exception:
        print Exception 
        print 'query types \n'
    for row in result:
        type = sparql.unpack_row(row)[0]

        typeLabel = getLabel(type)
        get_preds_query = get_predicates.replace('@id', type )
        #print get_preds_query
        try:
            preds = sparql.query(endpoint, get_preds_query)
        except Exception:
            print Exception 
            print 'query preds: ' + 'type ' + type 

        for predRow in preds:
            predicate = sparql.unpack_row(predRow)[0]
            relatedTypeQuery = get_related_types.replace('@type', type).replace('@pred', predicate)
            #print query
            try:
                relatedTypes = sparql.query(endpoint, relatedTypeQuery)
            except Exception:
                print Exception 
                print 'query related types: ' + 'type ' + type + ' predicate ' + predicate + '/n'
            

            predicateLabel = getLabel(predicate)
            hasResult = False
            for relatedTypeRow in relatedTypes:
                hasResult = True
                relatedType = sparql.unpack_row(relatedTypeRow)[0]
                relatedTypeCountQuery = get_related_types_count.replace('@type', type).replace('@pred',predicate).replace('@relatedType', relatedType)
                try:
                    relatedTypeCountResult = sparql.query(endpoint, relatedTypeCountQuery)
                except Exception:
                    print Exception 
                    print 'query relatedTypeCount' 

                relatedTypeLabel = getLabel(relatedType)
                for relatedTypeCountRow in relatedTypeCountResult:
                    tripleCount = sparql.unpack_row(relatedTypeCountRow)[0]
                    print type + ' -> ' + predicate + ' -> ' + relatedType + ' ' + str(tripleCount)
                    createCypher(type, typeLabel, predicate, predicateLabel, relatedType, relatedTypeLabel,
                                     tripleCount, False)
            if not hasResult:
                print type+' -> '+predicate+' -> LITERAL'
                createCypher(type, typeLabel, predicate, predicateLabel,  '', '', '', True)

session.close()
