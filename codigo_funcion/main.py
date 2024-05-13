# Copyright 2018 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the 'License');
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import base64
import json
import urllib
from os import getenv

import pymysql
from pymysql.err import OperationalError

CONNECTION_NAME = getenv('INSTANCE_CONNECTION_NAME', 'dev-bbva-workplace-monitoring:europe-west1:bbva-workplace-datalake')
DB_USER = getenv('MYSQL_USER')
DB_PASSWORD = getenv('MYSQL_PASSWORD')
DB_NAME = getenv('MYSQL_DATABASE', 'userinfo')
TOKEN = getenv('TOKEN')

mysql_config = {
    'user': DB_USER,
    'password': DB_PASSWORD,
    'db': DB_NAME,
    'charset': 'utf8mb4',
    'cursorclass': pymysql.cursors.DictCursor,
    'autocommit': True
}

print("mysql_config: --> ", mysql_config)
countries = {
    "ARGENTINA": "ARG",
    #"CHILE": "CHL",
    "CIB": "CIB",
    "COLOMBIA": "COL",
    "ESPAÑA": "ESP",
    "MÉXICO": "MEX",
    "PERÚ": "PER",
    "PORTUGAL": "ESP",
    "PARAGUAY": "PRY",
    #"TURQUÍA": "TUR",
    "URUGUAY": "URY",
    "USA": "USA",
    "ESTADOS UNIDOS": "USA",
    "VENEZUELA": "VEN",
}
# Create SQL connection globally to enable reuse
# PyMySQL does not include support for connection pooling
mysql_conn = None

def pubsub_userinfo_endpoint(request):
    if request.method == 'GET':
        return '<html><head><meta name="google-site-verification" content="' + TOKEN + '" /><title> Mi título </title></head><body>contenido de la página</body></html>', 200
    elif request.method == 'POST':
        message_userinfo = json.loads(urllib.parse.unquote(request.get_data().decode('utf-8')).rstrip('='))
        print ("entro por mensaje ")
        print (message_userinfo)
        try:
            input = json.loads(base64.b64decode(str(message_userinfo["message"]["data"])))
            print(input)
            action = input["action"]
            raw_message = input["data"]
            print(action)
            print(raw_message)
            supervisories=raw_message['supervisories']
            del raw_message['supervisories']
            jobProfile=raw_message['jobProfile']
            del raw_message['jobProfile']
            for x, y in supervisories.items():
                raw_message[x]=y

            for x, y in jobProfile.items():
                raw_message[x]=y
            print("final2 --> ")
            print(raw_message) 
        except:
            action = "ERROR" 
            print("Entro al error")
            raw_message = str(json.loads(base64.b64decode(str(message_userinfo["message"]["data"]))))

        mysql_demo()
        print("ya está conectado")
        with __get_cursor() as cursor:
            print("entro get_cursor")
            message = str(raw_message).replace('"', '\\"').replace('\n', '\\n').replace("'", "\\'")
            print(message)
            sql = 'INSERT INTO userinfotable_log (id, message) VALUES (DEFAULT, \'{}\')'.format(message)
            cursor.execute(sql)
            print("sql --> ")
            cursor.close()

        # Remember to close SQL resources declared while running this function.
        # Keep any declared in global scope (e.g. mysql_conn) for later reuse.

        keys = ['uid', 'codOUNivel9', 'codCentroTrabajo', 'employeeNumberLargo', 'codOUNivel8',
                'descOUNivel10', 'empresaempleado', 'descEmpresa', 'codOUNivel10', 'descpais', 'descOUNivel6', 'sn',
                'corporateNumber', 'descCentroCoste', 'descBancoOficinaPers', 'sn2', 'codBancoOficinaPers',
                'descCodPostalCentroTrabajo', 'codOUNivel3', 'givenName', 'unidadNegocio', 'descEstado', 'uidJefe',
                'uidSupervisor', 'uidGestor', 'firmaDigital', 'telephoneNumber', 'codCargo', 'codOUNivel5',
                'uidjefesecundario', 'descOUNivel7', 'codOUNivel6', 'codOUNivel4', 'descCentroTrabajo', 'postalCode',
                'codOUNivel7', 'codOUNivel2', 'descLargaPlanta', 'codCSB', 'desccsb', 'tipocodependencia', 'mail',
                'mobile', 'employeeType', 'descOUNivel8', 'codEstado', 'descOUNivel3', 'descOUNivel4', 'descOUNivel5',
                'bancobensoc', 'descOUNivel2', 'codOUNivel1', 'pager', 'codCentroCoste', 'idpaislocal', 'title',
                'descOUNivel9', 'descOUNivel1', 'codPostalCentroTrabajo', 'c', 'descempresaexterno', 'esempleado', 'o',
                'codoficina', 'descoficina', 'esresponsable', 'oubase', 'descoubase', 'sexo', 'fechanacimiento',
                'rangoglobal', 'empleadoVisible', 'visibilidad', 'nombrePreferido', 'apellidoPreferido', 'tipoContrato',
                'areagestion', 'codzonages', 'desczonages', 'plantilla', 'ambitogestion', 'codRol', 'descRol', 'status',
                'supervisoryOrganization', 'literalSupervisoryOrganization', 'supervisoryOrganization1', 'literalSupervisoryOrganization1', 'supervisoryOrganization2',
                'literalSupervisoryOrganization2', 'supervisoryOrganization3', 'literalSupervisoryOrganization3', 'supervisoryOrganization4', 'literalSupervisoryOrganization4', 
                'supervisoryOrganization5', 'literalSupervisoryOrganization5', 'supervisoryOrganization6', 'literalSupervisoryOrganization6', 'supervisoryOrganization7', 
                'literalSupervisoryOrganization7', 'supervisoryOrganization8', 'literalSupervisoryOrganization8', 'supervisoryOrganization9', 'literalSupervisoryOrganization9', 
                'supervisoryOrganization10', 'literalSupervisoryOrganization10', 'supervisoryOrganization11', 'literalSupervisoryOrganization11', 'supervisoryOrganization12', 
                'literalSupervisoryOrganization12', 'supervisoryOrganization13', 'literalSupervisoryOrganization13', 'literalSupervisoryOrganization14', 'literalSupervisoryOrganization15', 
                'managementLevel', 'buildingBlock', 'workerSubtype', 'jobFamily', 'jobFamilyGroup', 'buildingBlockDesc', 'workerSubtypeDesc', 'jobFamilyDesc', 'jobFamiliyGroupDesc', 'collectiveManager']
        print("Entro a get_clean_dict ")
        message = get_clean_dict(raw_message, keys)

        if action == 'CREATE':
            message['status'] = "C"
            with __get_cursor() as cursor:
                placeholders = ', '.join(['%s'] * len(message))
                columns = ', '.join(message.keys())
                sql = "INSERT INTO %s ( %s ) VALUES ( %s )" % ("userinfotable", columns, placeholders)
                try:
                    cursor.execute(sql, list(message.values()))
                    mysql_conn.commit()
                except:
                    sql = 'UPDATE userinfotable SET {}'.format(
                        ', '.join('{}=%s'.format(k) for k in message)) + " WHERE uid = '{}'".format(message.get('uid'))
                    cursor.execute(sql, list(message.values()))
                    mysql_conn.commit()
                cursor.close()
                return 'CREATE OK', 200
        elif action == 'UPDATE':
            message['status'] = "U"
            with __get_cursor() as cursor:
                sql = 'UPDATE userinfotable SET {}'.format(
                    ', '.join('{}=%s'.format(k) for k in message)) + " WHERE uid = '{}'".format(message.get('uid'))
                cursor.execute(sql, list(message.values()))
                mysql_conn.commit()
            cursor.close()
            return "UPDATE OK", 200
        elif action == 'DELETE':
            message['status'] = "D"
            with __get_cursor() as cursor:
                sql = "UPDATE userinfotable SET status= 'D' WHERE uid='{}'".format(message.get('uid'))
                cursor.execute(sql)
                mysql_conn.commit()
            cursor.close()
            return "DELETE OK", 200
        elif action == "ERROR":
            with __get_cursor() as cursor:
                message = str(message).replace('"', '\\"').replace('\n', '\\n').replace("'", "\\'")
                sql = "INSERT INTO userinfotable_error (id, error) VALUES (DEFAULT, '{}')".format(message)
                cursor.execute(sql)
                mysql_conn.commit()
            cursor.close()
            return "ERROR OK", 200
        else:
            with __get_cursor() as cursor:
                message = str(message).replace('"', '\\"').replace('\n', '\\n').replace("'", "\\'")
                sql = "INSERT INTO userinfotable_error (id, error) VALUES (DEFAULT, '{}')".format(message)
                cursor.execute(sql)
                mysql_conn.commit()
            cursor.close()
            return "ACTION UNAVAILABLE", 405
    else:
        return 'Method not allowed', 405


# [START functions_sql_mysql]
def __get_cursor():
    """
    Helper function to get a cursor
      PyMySQL does NOT automatically reconnect,
      so we must reconnect explicitly using ping()
    """
    try:
        return mysql_conn.cursor()
    except OperationalError:
        mysql_conn.ping(reconnect=True)
        return mysql_conn.cursor()


def mysql_demo():
    global mysql_conn

    # Initialize connections lazily, in case SQL access isn't needed for this
    # GCF instance. Doing so minimizes the number of active SQL connections,
    # which helps keep your GCF instances under SQL connection limits.
    if not mysql_conn:
        try:
            print("mysql_config: --> ", mysql_config)
            print("entrar a la conexión de mysql")
            mysql_conn = pymysql.connect(**mysql_config)
        except OperationalError:
            print("mysql_config: --> ", mysql_config)
            print("entrar a la conexión de mysql")
            # If production settings fail, use local development ones
            mysql_config['unix_socket'] = '/cloudsql/{CONNECTION_NAME}'.format(CONNECTION_NAME=CONNECTION_NAME)
            mysql_conn = pymysql.connect(**mysql_config)

    # Remember to close SQL resources declared while running this function.
    # Keep any declared in global scope (e.g. mysql_conn) for later reuse.
    with __get_cursor() as cursor:
        cursor.execute('SELECT NOW() as now')
        results = cursor.fetchone()
        return str(results['now'])


# [END functions_sql_mysql]

def get_sscc_red(row):
    """
    Given a row from userinfo returns if the user belongs to central services
    or branches
    """

    pais_clean = row['pais_clean']
    level2 = row['descOUNivel2']
    level3 = row['descOUNivel3']
    level4 = row['descOUNivel4']
    level5 = row['descOUNivel5']

    if pais_clean == 'ESP':
        if (level3 == 'DIRECCION RED BANCA COMERCIAL') or (level3 == 'DIRECCION RED BEC'):
            return 'RED'
        else:
            return 'SSCC'

    if pais_clean == 'CIB':
        return 'SSCC'

    if pais_clean == 'HLD':
        return 'SSCC'

    if pais_clean == 'MEX':
        red_mex = ['DG BCA DE EMPRESAS Y GOBIERNO', 'DG RED COMERCIAL']  # 'DG BCA MAYORISTA Y D INVERSION'
        if (level2 in red_mex) or (level3 in red_mex):
            return 'RED'
        return 'SSCC'

    if pais_clean == 'USA':
        if level2 == 'COMMERCIAL BANKING':
            return 'RED'
        return 'SSCC'

    if ((pais_clean == 'ARG' and level4 == 'COMERCIAL') or
            (pais_clean == 'COL' and level4 == 'DIRECCION DE REDES') or
            (pais_clean == 'PER' and level4 in ['BANCA MINORISTA',
                                                'BANCA EMPRESA Y CORPORATIVA']) or  ##### 'DISTRIBUCION RED'
            (pais_clean == 'PRY' and level5 == 'CHIEF COMMERCIAL OFFICER') or  ##### 'DIRECCION DE REDES',
            (pais_clean == 'URY' and level5 == 'BUSINESS EXECUTION CS') or  ##### 'UNIDAD COMERCIAL'
            (pais_clean == 'VEN' and level4 == 'BANCA COMERCIAL')):
        return 'RED'

    return 'SSCC'


def get_pais_clean(row, cib_esp_redext=True):
    """
    Given a row from userinfo returns the organizational country
    associated to that user

    If 'cib_esp_redext' == True, only geography Spain and foreign network
    (red exterior) could be marked as 'CIB'.
    Otherwise, if 'cib_esp_redext' == False, any country could be marked as 'CIB'
    """

    country_geo = row.get('descpais')
    level1 = row.get('descOUNivel1')
    level2 = row.get('descOUNivel2')

    #### CIB ####
    if cib_esp_redext == True:

        # list of countries that should be excluded from CIB
        noCIB = ['ARGENTINA', 'CHILE', 'COLOMBIA', 'MÉXICO', 'PERÚ', 'PARAGUAY',
                 'URUGUAY', 'USA', 'ESTADOS UNIDOS', 'VENEZUELA']

        if ((level1 == 'CORPORATE & INVESTMENT BANKING') or (level2 == 'CORPORATE & INVESTMENT BANKING')) and (
                country_geo in noCIB):
            return countries[country_geo]

    if (level1 == 'CORPORATE & INVESTMENT BANKING') or (level2 == 'CORPORATE & INVESTMENT BANKING'):
        return 'CIB'

    #### ESPAÑA, HOLDING ####
    if country_geo == 'ESPAÑA':

        eyp1 = ['COUNTRY NETWORKS', 'COUNTRY MANAGER SPAIN', 'COUNTRIES',
                'BUSINESS DEVELOPMENT SPAIN']  # Nivel1
        eyp2 = ['BUSINESS DEVELOPMENT SPAIN']  # Nivel2

        if (level1 in eyp1) or (level2 in eyp2):
            return 'ESP'
        else:
            return 'HLD'

    # some employees in BELGICA, FRANCIA, REINO UNIDO and ITALIA depend on Spain
    if level1 == 'COUNTRY MANAGER SPAIN':
        return 'ESP'

    # some employees in foreign countries depend on HOLDING
    if country_geo in ['ALEMANIA', 'BELGICA', 'CHINA', 'FRANCIA', 'HOLANDA',
                       'HONG-KONG', 'IRLANDA', 'ITALIA', 'REINO UNIDO', 'RUMANIA',
                       'SINGAPUR', 'SUIZA', 'TAIWÁN  PROVINCIA CHINA']:
        return 'HLD'

    try:
        return countries.get(country_geo)
    except KeyError:
        return 'OTH'


def get_area_clean(row):
    """
    Given a row from userinfo returns the organizational unit (area)
    associated to that user
    """
    pais_clean = row['pais_clean']

    if row['descOUNivel1'] == 'CLIENT SOLUTIONS':
        return "OTH"

    if pais_clean == "MEX":
        dict_areas = {
            'COUNTRY MANAGER MEXICO': 'COUNTRY MONITORING',
            'CUMPLIMIENTO': 'LEGAL SERVICES & COMPLIANCE',
            'DG BCA DE EMPRESAS Y GOBIERNO': 'COMMERCIAL BANKING',
            'DG ENGINEERING & HOD MX': 'ENGINEERING & HOD',
            'DG EXPERIENCIA ÚNICA': 'DG EXPERIENCIA ÚNICA',
            'DG RED COMERCIAL': 'RED COMERCIAL',
            'DG RIESGOS MEXICO': 'RISK MANAGEMENT',
            'DG SERVICIOS JURIDICOS': 'LEGAL SERVICES & COMPLIANCE',
            'DG TALENTO & CULTURA': 'TALENT & CULTURE',
            'FINANZAS': 'FINANCE',
            'JEFE GABINETE': 'PRESIDENCIA',
            'VARIOS': 'VARIOS'
        }

        if row['descOUNivel1'] == 'CORPORATE & INVESTMENT BANKING':
            return "CIB"
        elif row['descOUNivel1'] == 'INTERNAL AUDIT':
            return 'INTERNAL AUDIT'
        elif row['descOUNivel1'] == 'GLOBAL ECONOMICS & PA':
            return 'GLOBAL ECONOMICS & PA'
        else:
            try:
                return dict_areas.get(row['descOUNivel2'])
            except KeyError:
                return row['descOUNivel2']

    elif pais_clean == "ARG":
        dict_areas = {
            'ARGENTINA': 'COUNTRY MONITORING',
            'COMERCIAL': 'COMMERCIAL BANKING',
            'CUMPLIMIENTO': 'LEGAL SERVICES & COMPLIANCE',
            'FINANZAS': 'FINANCE',
            'FUNDACION': 'BBVA FOUNDATION',
            'GABINETE': 'PRESIDENCIA',
            'INGENIERIA & DATA': 'ENGINEERING & HOD',
            'RELACIONES INSTITUCIONALES': 'GLOBAL ECONOMICS & PA',
            'RIESGOS': 'RISK MANAGEMENT',
            'SERVICIOS JURIDICOS': 'LEGAL SERVICES & COMPLIANCE',
            'TALENTO & CULTURA': 'TALENT & CULTURE'
        }

        if row['descOUNivel1'] == 'CORPORATE & INVESTMENT BANKING':
            return "CIB"
        elif row['descOUNivel1'] == 'INTERNAL AUDIT':
            return 'INTERNAL AUDIT'
        elif row['descOUNivel1'] == 'GLOBAL ECONOMICS & PA':
            return 'GLOBAL ECONOMICS & PA'
        else:
            try:
                return dict_areas.get(row['descOUNivel4'])
            except KeyError:
                return row['descOUNivel4']

    elif pais_clean == "USA":
        dict_areas = {
            'BFH': 'BFH',
            'CHIEF FINANCIAL OFFICER': 'FINANCE',
            'COMMERCIAL BANKING': 'COMMERCIAL BANKING',
            'COMPASS CHIEF EXECUTIV OFFICER': 'PRESIDENCIA',
            'COMPLIANCE': 'LEGAL SERVICES & COMPLIANCE',
            'CORP.RESPONSABILITY&REPUTATION': 'COMMUNICATIONS',
            'COUNTRY MANAGER USA': 'COUNTRY MONITORING',
            'EAST': 'EAST',
            'ENGINEERING': 'ENGINEERING & HOD',
            'FINANCE': 'FINANCE',
            'GENERAL COUNSEL': 'LEGAL SERVICES & COMPLIANCE',
            'RETAIL BANKING': 'RETAIL BANKING',
            'RISK MANAGEMENT': 'RISK MANAGEMENT',
            'STRATEGY AND PLANNING & HOD': 'ENGINEERING & HOD',
            'TALENT & CULTURE': 'TALENT & CULTURE',
            'USUARIOS REUBICAR': 'VARIOS',
            'VICE CHAIRMAN BBVA COMPASS': 'PRESIDENCIA'
        }

        if row['descOUNivel1'] == 'CORPORATE & INVESTMENT BANKING':
            return "CIB"
        elif row['descOUNivel1'] == 'INTERNAL AUDIT':
            return 'INTERNAL AUDIT'
        elif row['descOUNivel1'] == 'GLOBAL ECONOMICS & PA':
            return 'GLOBAL ECONOMICS & PA'
        else:
            try:
                return dict_areas.get(row['descOUNivel2'])
            except KeyError:
                return row['descOUNivel2']

    elif pais_clean == "PER":
        dict_areas = {
            'BANCA EMPRESA Y CORPORATIVA': 'COMMERCIAL BANKING',
            'BANCA MINORISTA': 'COMMERCIAL BANKING',
            'BUS. PROCESS ENGINEERING & HOD': 'ENGINEERING & HOD',
            'ENGINEERING': 'ENGINEERING & HOD',
            'FINANZAS': 'FINANCE',
            'IMAGEN Y COMUNICACION': 'COMMUNICATIONS',
            'PERU': 'COUNTRY MONITORING',
            'RIESGOS': 'RISKS',
            'SERVICIOS JURIDICOS': 'LEGAL SERVICES & COMPLIANCE',
            'TALENT & CULTURE': 'TALENT & CULTURE',
            'VARIOS': 'VARIOS'
        }

        if row['descOUNivel1'] == 'CORPORATE & INVESTMENT BANKING':
            return "CIB"
        elif row['descOUNivel1'] == 'INTERNAL AUDIT':
            return 'INTERNAL AUDIT'
        elif row['descOUNivel1'] == 'GLOBAL ECONOMICS & PA':
            return 'GLOBAL ECONOMICS & PA'
        else:
            try:
                return dict_areas.get(row['descOUNivel4'])
            except KeyError:
                return row['descOUNivel4']

    elif pais_clean == "PRY":
        dict_areas = {
            'ASISTENTE DE PRESIDENCIA': 'PRESIDENCIA',
            'CHIEF COMMERCIAL OFFICER': 'COMMERCIAL BANKING',
            'CHIEF OPERATION OFFICER & HOD': 'ENGINEERING & HOD',
            'COMPLIANCE': 'LEGAL SERVICES & COMPLIANCE',
            'FINANCE': 'FINANCE',
            'LEGAL': 'LEGAL SERVICES & COMPLIANCE',
            'PARAGUAY': 'COUNTRY MONITORING',
            'RISK': 'RISK MANAGEMENT',
            'TALENT & CULTURE': 'TALENT & CULTURE'
        }

        if row['descOUNivel1'] == 'INTERNAL AUDIT':
            return 'INTERNAL AUDIT'
        else:
            try:
                return dict_areas.get(row['descOUNivel5'])
            except KeyError:
                return row['descOUNivel5']

    elif pais_clean == "URY":
        dict_areas = {
            'AREAS DE PRESIDENCIA': 'PRESIDENCIA',
            'ASESORIA LEGAL': 'LEGAL SERVICES & COMPLIANCE',
            'BBVA DISTRIBUIDORA DE SEGUROS': 'BBVA DISTRIBUIDORA DE SEGUROS',
            'BUSINESS EXECUTION CS': 'COMMERCIAL BANKING',
            'COMPRAS HUB COMPRAS': 'FINANCE',
            'CUSTOMER SOLUTIONS': 'CLIENT SOLUTIONS',
            'EMPRENDIMIENTOS DE VALOR S.A.': 'EMPRENDIMIENTOS DE VALOR S.A.',
            'ENGINEERING & HOD': 'ENGINEERING & HOD',
            'SECRETARIA GENERAL': 'PRESIDENCIA',
            'TALENT & CULTURE': 'TALENT & CULTURE',
            'UNIDAD DE RIESGOS': 'RISK MANAGEMENT',
            'UNIDAD FINANCIERA': 'FINANCE',
            'URUGUAY': 'COUNTRY MONITORING'
        }

        if row['descOUNivel1'] == 'INTERNAL AUDIT':
            return 'INTERNAL AUDIT'
        else:
            try:
                return dict_areas.get(row['descOUNivel5'])
            except KeyError:
                return row['descOUNivel5']

    elif pais_clean == "COL":
        dict_areas = {
            'COLOMBIA': 'COUNTRY MONITORING',
            'COMUNICACIÓN E IMAGEN': 'COMMUNICATIONS',
            'DIRECCION DE REDES': 'COMMERCIAL BANKING',
            'FINANCIERA': 'FINANCE',
            'INGENIERÍA & HOD': 'ENGINEERING & HOD',
            'RIESGOS': 'RISK MANAGEMENT',
            'SERVICIOS JURÍDICOS Y SECRETAR': 'LEGAL SERVICES & COMPLIANCE',
            'TALENT & CULTURE': 'TALENT & CULTURE',
            'VARIOS': 'VARIOS'
        }

        if row['descOUNivel1'] == 'CORPORATE & INVESTMENT BANKING':
            return "CIB"
        elif row['descOUNivel1'] == 'INTERNAL AUDIT':
            return 'INTERNAL AUDIT'
        elif row['descOUNivel1'] == 'GLOBAL ECONOMICS & PA':
            return 'GLOBAL ECONOMICS & PA'
        elif row['descOUNivel3'] == 'CLIENT SOLUTIONS':
            return 'CLIENT SOLUTIONS'
        elif row['descOUNivel3'] == 'GERENCIA GENERAL SEGUROS':
            return 'GERENCIA GENERAL SEGUROS'
        else:
            try:
                return dict_areas.get(row['descOUNivel4'])
            except KeyError:
                return row['descOUNivel4']

    elif pais_clean == "VEN":
        dict_areas = {
            'BANCA COMERCIAL': 'COMMERCIAL BANKING',
            'COMPRAS HUB COMPRAS': 'FINANCE',
            'ENGINEERING': 'ENGINEERING & HOD',
            'FINANCIERA': 'FINANCE',
            'RIESGOS': 'RISK MANAGEMENT',
            'SERVICIOS JURIDICOS': 'LEGAL SERVICES & COMPLIANCE',
            'TALENTO Y CULTURA': 'TALENT & CULTURE',
            'VARIOS-APOYO C.D.': 'VARIOS',
            'VARIOS': 'VARIOS',
            'VENEZUELA': 'COUNTRY MONITORING'
        }

        if row['descOUNivel1'] == 'CORPORATE & INVESTMENT BANKING':
            return "CIB"
        elif row['descOUNivel1'] == 'INTERNAL AUDIT':
            return 'INTERNAL AUDIT'
        elif row['descOUNivel1'] == 'GLOBAL ECONOMICS & PA':
            return 'GLOBAL ECONOMICS & PA'
        else:
            try:
                return dict_areas.get(row['descOUNivel4'])
            except KeyError:
                return row['descOUNivel4']

    elif pais_clean == "HLD":
        dict_areas = {
            'BBVA FOUNDATION': 'BBVA FOUNDATION',
            'CHIEF EXECUTIVE OFFICER': 'PRESIDENCIA',
            'CLIENT SOLUTIONS': 'CLIENT SOLUTIONS',
            'COMMUNICATIONS': 'COMMUNICATIONS',
            'COMMUNICATIONS & RESP. BUSINES': 'COMMUNICATIONS',
            'COUNTRY MONITORING': 'COUNTRY MONITORING',
            'DATA': 'DATA',
            'ENGINEERING': 'ENGINEERING & HOD',
            'ENGINEERING & ORGANIZATION': 'ENGINEERING & HOD',
            'FINANCE': 'FINANCE',
            'FUNDACION BBVA MICROFINANZAS': 'FUNDACION BBVA MICROFINANZAS',
            'GENERAL SECRETARY': 'PRESIDENCIA',
            'GLOBAL ECONOMICS & PA': 'GLOBAL ECONOMICS & PA',
            'GLOBAL RISK MANAGEMENT': 'RISK MANAGEMENT',
            'GROUP EXECUTIVE CHAIRMAN': 'PRESIDENCIA',
            'HEAD OF LEGAL': 'LEGAL SERVICES & COMPLIANCE',
            'INTERNAL AUDIT': 'INTERNAL AUDIT',
            'REGULATION & INTERNAL CONTROL': 'REGULATION & INTERNAL CONTROL',
            'SENIOR ADVISOR TO THE CHAIRMAN': 'PRESIDENCIA',
            'STRATEGY & M&A': 'STRATEGY & M&A',
            'TALENT & CULTURE': 'TALENT & CULTURE',
            'VARIOS': 'VARIOS'
        }

        try:
            return dict_areas.get(row['descOUNivel1'])
        except KeyError:
            return row['descOUNivel1']

    elif pais_clean == "ESP":
        dict_areas_cms = {
            'COMUNICACION Y NEGOCIO RESPON.': 'COMMUNICATIONS',
            'COUNTRY MANAGER SPAIN': 'COUNTRY MONITORING',
            'CUMPLIMIENTO BBVA ESPAÑA': 'LEGAL SERVICES & COMPLIANCE',
            'DIRECCION RED BANCA COMERCIAL': 'COMMERCIAL BANKING',
            'DIRECCION RED BEC': 'COMMERCIAL BANKING',
            'FINANZAS': 'FINANCE',
            'GRM SPAIN': 'GRM SPAIN',
            'INGENIERIA & HOD ESPAÑA': 'ENGINEERING & HOD',
            'SS.JJ.  ESPAÑA': 'LEGAL SERVICES & COMPLIANCE',
            'TALENTO Y CULTURA ESPAÑA': 'TALENT & CULTURE'
        }

        if row['descOUNivel1'] == 'COUNTRY MANAGER SPAIN':
            try:
                return dict_areas_cms.get(row['descOUNivel3'])
            except KeyError:
                return row['descOUNivel3']

        elif row['descOUNivel1'] == 'BUSINESS DEVELOPMENT SPAIN':
            return 'BUSINESS DEVELOPMENT SPAIN'
        else:
            return 'OTH'

    elif pais_clean == "CIB":
        dict_areas = {
            'CIB GEOGRAP.&GLOBAL CROSS BORD': 'CIB GEOGRAP.&GLOBAL CROSS BORD',
            'CORPORATE & INVESTMENT BANKING': 'CIB',
            'ENGINEERING & HOD CIB': 'ENGINEERING & HOD',
            'FINANCE  G.PLANNING & T&C CIB': 'FINANCE',
            'FINANCE, G.PLANNING & T&C CIB': 'TALENT & CULTURE',
            'GLOBAL CLIENTS': 'GLOBAL CLIENTS',
            'GLOBAL FINANCE': 'FINANCE',
            'GLOBAL MARKETS': 'GLOBAL MARKETS',
            'GLOBAL TRANSACTION BANKING': 'GLOBAL TRANSACTION BANKING',
            'GRM DISCIPLINES & FRONT': 'GRM DISCIPLINES & FRONT',
            'LEGAL FRONT CIB/ CAPITAL MKTS': 'LEGAL SERVICES & COMPLIANCE',
            'RISK MANAGEMENT GROUP': 'RISK MANAGEMENT',
            'RISK SOLUTION GROUP': 'RISK MANAGEMENT'
        }

        try:
            return dict_areas.get(row['descOUNivel3'])
        except KeyError:
            return row['descOUNivel3']

    else:
        return "OTH"


def get_clean_dict(y, keys):
    x = {}
    print("dentro de get_clean_dict")
    for key in keys:
        try:
            x[key] = y.get(key)
            print(x[key])
        except KeyError:
            x[key] = ""
        except AttributeError:
            pass
    return x
