#!/usr/bin/env python

###########################################################################
##                                                                       ##
## Copyrights Frédéric Rodrigo 2016                                      ##
##                                                                       ##
## This program is free software: you can redistribute it and/or modify  ##
## it under the terms of the GNU General Public License as published by  ##
## the Free Software Foundation, either version 3 of the License, or     ##
## (at your option) any later version.                                   ##
##                                                                       ##
## This program is distributed in the hope that it will be useful,       ##
## but WITHOUT ANY WARRANTY; without even the implied warranty of        ##
## MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the         ##
## GNU General Public License for more details.                          ##
##                                                                       ##
## You should have received a copy of the GNU General Public License     ##
## along with this program.  If not, see <http://www.gnu.org/licenses/>. ##
##                                                                       ##
###########################################################################

import xml.etree.ElementTree as ET
from pathlib import Path

from collections import namedtuple
# from recordtype import recordtype (for mutalbe namedtuple)

from modules.OsmoseTranslation import T_
from .Analyser_Osmosis import Analyser_Osmosis

def xml_to_sql(node, table_name=None):
    ref_table = ''
    if table_name:
        ref_table = f'{table_name}.'

    if node.tag == 'and':
        eval_seq = [xml_to_sql(el, table_name) for el in node]
        ret = ') AND ('.join(eval_seq)
        return '(' + ret + ')'
    elif node.tag == 'or':
        eval_seq = [xml_to_sql(el, table_name) for el in node]
        ret = ') OR ('.join(eval_seq)
        return '(' + ret + ')'
    elif node.tag == 'has_tag':
        return f"{ref_table}tags?'{node.text}'"
    elif node.tag == 'key_value':
        if node.text.find("!=") > 0:
            k, v = node.text.split("!=")
            return f"{ref_table}tags->'{k}'!='{v}'"
        elif node.text.find("=") > 0:
            k, v = node.text.split("=")
            return f"{ref_table}tags->'{k}'='{v}'"
    elif node.tag == 'has_not_tag':
        return f"not {ref_table}tags?'{node.text}'"
    elif node.tag == 'intersects':
        return f"st_intersects({ref_table}geom, {node.text}.geom)"
    else:
        print(f'ERROR {node.tag}')

analysers_path = "/opt/osmose-backend/analysers/xml-analysers/"

basic_requests = {
    'nodes': {'obj': 'node', 'geom': 'point'},
    'ways_linestring': {'obj': 'way', 'geom': 'linestring'},
    'ways_bbox': {'obj': 'way', 'geom': 'polygon'},
    'relations_polygon': {'obj': 'relation', 'geom': 'polygon'},
}

RequestStackElem = namedtuple('RequestStackElem', ['name', 'obj', 'geom'])

class XMLConfigError(Exception):
    """Base class for exceptions in this module."""
    pass

class XMLRequest():
    def __init__(self, xml_node):
        self.name = xml_node.find('name').text # name de la request (creation de table temporaire)
        self.obj = xml_node.find('obj').text # type osm renvoyé : node, way, relation
        self.geom = xml_node.find('geom').text # type de la geometry (pour relation : point, linestring, polygon, multipolygon)
        self.from_ = xml_node.find('from').text # from

        # si commence par buffer 
        # self.buffer = True 
        # sinon pas buffer
        self.filter = None
        self.buffer = None
        self.join_with = set() # table avec lequelles faire les jointures

        # is filter or buffer
        filter_node = xml_node.find('filter')
        if filter_node:
            self.filter = xml_to_sql(filter_node[0], 'el')

            # si besoin de faire des jointures
            for intersects_node in filter_node.iter('intersects'):
                self.join_with.add(intersects_node.text)

        else: # this is a buffer rule:
            buffer_node = xml_node.find('buffer')
            self.buffer = buffer_node.text


        
    def is_buffer(self): # otherwith it is a buffer request
        return bool(self.buffer)


    def to_sql(self, last=False):
        if self.is_buffer():
            if last:
                raise XMLConfigError()

            sql = f"""
CREATE TEMP TABLE {self.name} AS
SELECT
    el.id as id,
    st_buffer(el.geom, {self.buffer}) as geom,
    '{self.obj}' as obj
FROM {self.from_} as el;
"""
            return sql
                
        # Jointures
        join = ''
        for t in self.join_with:
            join = f"{join} CROSS JOIN {t}"


        if not last:
            sql = f"""
CREATE TEMP TABLE {self.name} AS
SELECT DISTINCT
    el.id as id,
    '{self.obj}' as obj,
    el.geom as geom
FROM {self.from_} as el {join}
WHERE {self.filter}
    """
            return sql
        
        # (last & node)
        if self.obj == 'node':
            sql = f"""
SELECT DISTINCT
    el.id as id,
    ST_AsText(el.geom) as geom
FROM {self.from_} as el {join}
WHERE {self.filter}
    """
            return sql

        # (last & not node)
        sql = f"""
SELECT DISTINCT
        el.id as id
FROM {self.from_} as el {join}
WHERE {self.filter}
    """
        return sql


class XMLClass():
    def __init__(self, xml_node):
        self.id = xml_node.find('id').text
        self.item = xml_node.find('item').text
        self.level = xml_node.find('level').text
        self.title = xml_node.find('title').text


class XMLAnalyser():
    def __init__(self, xml_node):
        self.classs = XMLClass(xml_node.find('class'))

        self.requests = dict()
        self.last_request = None

        for req_node in xml_node.find('requests'):
            req = XMLRequest(req_node)
            self.requests[req.name] = req
            self.last_request = req

    def requests_stack(self):
        # compute the stack of request to coompute
        def iter_requests_stack(req):
            # todo verifier que les joins (si pas déjà dedans)
            # faire des name tuples poour (req.from_, basic_req.obj, basic_req.geom)
            stack = None

            if req.from_ in basic_requests:
                basic_req = basic_requests[req.from_]
                stack = [RequestStackElem(req.from_, basic_req['obj'], basic_req['geom'])]
            else:
                stack = iter_requests_stack(self.requests[req.from_])

            req_stack_elem = RequestStackElem(req.name, stack[-1].obj, stack[-1].geom)
            if req.is_buffer: # this is buffer:
                # change geom to polygon
                req_stack_elem = RequestStackElem(req_stack_elem.name, req_stack_elem.obj, 'polygon')

            for join_with_req_name in req.join_with:
                already_in_stack = False

                for stack_req in stack:
                    if join_with_req_name == stack_req:
                        already_in_stack = True

                
                if not already_in_stack:
                    if join_with_req_name in basic_requests:
                        basic_req = basic_requests[join_with_req_name]
                        stack.append(RequestStackElem(req.from_, basic_req['obj'], basic_req['geom']))
                    else:
                        join_req_stack = iter_requests_stack(self.requests[join_with_req_name])
                        stack = stack + join_req_stack # todo améliorer juste ceux qui ne se trouv pas deja dans stack
            
            stack.append(req_stack_elem)
            return stack

        return iter_requests_stack(self.last_request)


class Analyser_Osmosis_From_XML(Analyser_Osmosis):
    def __init__(self, config, logger = None):
        Analyser_Osmosis.__init__(self, config, logger)

        analysers_dir = Path(analysers_path)
        for analyser_xml in analysers_dir.iterdir():
            if analyser_xml.is_file():
                with analyser_xml.open() as xml:
                    xml_analyser = ET.parse(xml)
                    analyser = XMLAnalyser(xml_analyser)

                    self.classs[analyser.classs.id] = self.def_class(
                        item = analyser.classs.item,
                        level = analyser.classs.level,
                        tags = [],
                        title = T_(analyser.classs.title),
                        # fix = T_('TODO'),
                        # trap = T_('TODO'))
                    )
 
    def analyser_osmosis_common(self):

        analysers_dir = Path(analysers_path)
        for analyser_xml in analysers_dir.iterdir():
            if analyser_xml.is_file():
                with analyser_xml.open() as xml:
                    xml_analyser = ET.parse(xml)
                    analyser = XMLAnalyser(xml_analyser)

                    req = """
CREATE TEMP TABLE relations_polygon AS
SELECT
    el.id AS id,
    ST_Buffer(ST_Polygonize(ways.linestring), 0) AS geom,
    el.tags
FROM
    relations AS el
    JOIN relation_members ON
        relation_members.relation_id = el.id AND
        relation_members.member_type = 'W'
    JOIN ways ON
        ways.id = relation_members.member_id AND
        ST_NPoints(ways.linestring) > 1
GROUP BY
    el.id;"""

                    self.run(req)

                    requests_stack = analyser.requests_stack()
                    for r in requests_stack[:-1]:
                        if r.name in basic_requests:
                            # this is a basic request
                            pass
                        else:
                            req = analyser.requests[r.name]
                            self.run(req.to_sql())

                    last_stack_elem = requests_stack[-1]
                    last_req = analyser.requests[last_stack_elem.name]

                    if(last_req.obj == 'node'):
                        cb = lambda res: {
                            "class": analyser.classs.id,
                            "data": [self.node_full, self.positionAsText]
                        }
                        self.run(last_req.to_sql(last=True), cb)
                    elif(last_req.obj == 'way'):
                        cb = lambda res: {
                            "class": analyser.classs.id,
                            "data": [self.way_full]
                        }
                        self.run(last_req.to_sql(last=True), cb)
                    elif(last_req.obj == 'relation'):
                        cb = lambda res: {
                            "class": analyser.classs.id,
                            "data": [self.relation_full]
                        }
                        self.run(last_req.to_sql(last=True), cb)
