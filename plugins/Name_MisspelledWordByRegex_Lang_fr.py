#-*- coding: utf-8 -*-

###########################################################################
##                                                                       ##
## Copyrights Etienne Chové <chove@crans.org> 2009                       ##
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

from plugins.Name_MisspelledWordByRegex import P_Name_MisspelledWordByRegex


class Name_MisspelledWordByRegex_Lang_fr(P_Name_MisspelledWordByRegex):

    only_for = ["fr"]

    def init(self, logger):
        P_Name_MisspelledWordByRegex.init(self, logger)

        import re
        self.ReTests = {}
        self.ReTests[( 0, u"École\\2")]      = [re.compile(r"^École(| .*)$"),
                                               re.compile(r"^([EÉée][Cc][Oo][Ll][Ee])(| .*)$")]
        self.ReTests[( 1, u"Église\\2")]     = [re.compile(r"^Église(| .*)$"),
                                               re.compile(r"^([EÉée][Gg][l][Ii][Ss][Ee])(| .*)$")]
        self.ReTests[( 2, u"La\\2")]         = [re.compile(r"^La(| .*)$"),
                                               re.compile(r"^([Ll][Aa])( .*)$")]
        self.ReTests[( 3, u"Étang\\2")]      = [re.compile(r"^Étang(| .*)$"),
                                               re.compile(r"^([EÉée][Tt][Tt]?[AaEe][Nn][GgTt]?)(| .*)$")]
        self.ReTests[( 4, u"Saint\\2")]      = [re.compile(r"^Saint(| .*)$"),
                                               re.compile(r"^([Ss](?:[Aa][Ii][Nn])?[Tt]\.?)( .*)$")]
        self.ReTests[( 5, u"Hôtel\\2")]      = [re.compile(r"^Hôtel(| .*)$"),
                                               re.compile(r"^([Hh][OoÔô][Tt][Ee][Ll])(| .*)$")]
        self.ReTests[( 6, u"Château\\2")]    = [re.compile(r"^Château(| .*)$"),
                                               re.compile(r"^([Cc][Hh][ÂâAa][Tt][Ee][Aa][Uu])(| .*)$")]
        self.ReTests[( 7, u"McDonald's\\2")] = [re.compile(r"^McDonald's(| .*)$"),
                                               re.compile(r"^([Mm][Aa]?[Cc] ?[Dd][Oo](?:[Nn][Aa][Ll][Dd]'?[Ss]?)?)(| .+)$")]
        self.ReTests[( 8, u"Sainte\\2")]     = [re.compile(r"^Sainte(| .*)$"),
                                               re.compile(r"^([Ss](?:[Aa][Ii][Nn])?[Tt][Ee]\.?)( .*)$")]
        self.ReTests[( 9, u"Le\\2")]         = [re.compile(r"^Le(| .*)$"),
                                               re.compile(r"^([Ll][Ee])( .*)$")]
        self.ReTests[(10, u"Les\\2")]        = [re.compile(r"^Les(| .*)$"),
                                               re.compile(r"^([Ll][Ee][Ss])( .*)$")]
        self.ReTests[(11, u"\\1\\2'\\4")]    = [re.compile(r"[LlDd]'(|[^ ].*)$"),
                                               re.compile(r"(^|.* )([LlDd])( +' +| +'|' +)(|.*)$")]
        self.ReTests = self.ReTests.items()


###########################################################################
from plugins.Plugin import TestPluginCommon

class Test(TestPluginCommon):
    def test(self):
        a = Name_MisspelledWordByRegex_Lang_fr(None)
        a.init(None)
        for (d, f) in [(u"eglise ", u"Église "),
                       (u"St. Michel", u"Saint Michel"),
                       (u"Ecole", u"École"),
                       (u"Mac Donald", u"McDonald's"),
                       (u"Ste Amal et Fils Sarl", u"Sainte Amal et Fils Sarl"),
                       (u"SAiNte anne", u"Sainte anne"),
                       (u"les lesles", u"Les lesles"),
                       (u"de l' été", u"de l'été"),
                       (u"de l' ", u"de l'"),
                       (u"de l 'été", u"de l'été"),
                       (u"de l '", u"de l'"),
                       (u"l ' été", u"l'été"),
                      ]:
            self.check_err(a.node(None, {"name": d}), ("name='%s'" % d))
            self.assertEqual(a.node(None, {"name": d})["fix"]["name"], f)
            assert not a.node(None, {"name": f}), ("name='%s'" % f)

            self.check_err(a.way(None, {"name": d}, None), ("name='%s'" % d))
            self.check_err(a.relation(None, {"name": d}, None), ("name='%s'" % d))
            assert not a.node(None, {"amenity": d}), ("amenity='%s'" % d)
