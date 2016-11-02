# coding: utf-8
from multiprocessing.dummy import Pool
from multiprocessing import cpu_count, TimeoutError, Manager
import re
import sys
import time
import traceback
import warnings

from lxml import html
import numpy as np
import pandas as pd
import requests
from tqdm import tqdm, trange


def scrapePage(pageString):
    # parse html
    tree = html.fromstring(pageString)

    # get main div
    cntDetail = tree.xpath('/html/body/div[@id="main"]/div[@id="cnt"]/div[@id="cntDetail"]')[0]

    # initialize output dictionary
    output = {}

    def extractTable(liList, spec, labelIdx=0, valIdx=1):
        output = {}
        assert len(liList) == len(spec)
        for i, (var, label) in enumerate(spec):
            #print(var)
            #print(label)
            #print(liList[i][labelIdx].text)
            assert liList[i][labelIdx].text == label
            val = liList[i][valIdx].text
            output[var] = val
        return(output)


    # Detalles del Expediente section
    curList = cntDetail.xpath('h3[text()="Detalles del Expediente"]'
                             '/following-sibling::div[@class="form_container"][1]/ul/li')
    curSpec = [('CODIGO_EXPEDIENTE', 'Código del Expediente'),
               ('TITULO_EXPEDIENTE', 'Descripción del Expediente')]
    output.update(extractTable(curList, curSpec))

    # Detalles del Anuncio section
    curList = cntDetail.xpath('h3[text()="Detalles del Anuncio"]'
                             '/following-sibling::div[@class="form_container"][1]/ul/li')
    curSpec = [('_DESC_ANUNCIO', 'Descripción del Anuncio'),
               ('_NOTAS', 'Notas'),
               ('TIPO_CONTRATACION', 'Tipo de Contratación'),
               ('_ENTIDAD_FEDERATIVA', 'Entidad Federativa'),
               ('FECHA_APERTURA_PROPOSICIONES', 'Plazo de participación o vigencia del anuncio')
              ]
    output.update(extractTable(curList, curSpec))

    # conditionalPrefix table
    # variation 1, contains only Detalles del Procedimiento section
    curList = cntDetail.xpath('table[@id="conditionalPrefixFor_92"]/tr')
    if len(curList) > 0:
        output['_CONDITIONAL_PREFIX_FOR'] = 92
        assert curList[0].xpath('th/h4')[0].text == 'Detalles del Procedimiento'
        assert curList[1].attrib['class'] == 'accessHidden'
        curList = curList[2:]
        curSpec = [(var, '\n' + label) for (var, label) in
                    [('NUMERO_PROCEDIMIENTO', 'Número del Procedimiento (Anuncio)'),
                     ('CARACTER', 'Carácter del procedimiento'),
                     ('_CREDITO_EXTERNO', 'Crédito externo'),
                     ('FORMA_PROCEDIMIENTO', 'Medio o forma del procedimiento'),
                     ('_EXCLUSIVO_MIPYMES', 'Procedimiento exclusivo para MIPYMES'),
                     ('PROC_F_PUBLICACION', 'Fecha de publicación del anuncio'
                      ' (Convocatoria / Invitación / Adjudicación / Proyecto de Convocatoria)')
                    ]
                  ]
        output.update(extractTable(curList, curSpec, labelIdx=1, valIdx=3))
    # variation 2, contains Detalles del Procedimiento and Anexos sections
    curList = cntDetail.xpath('table[@id="conditionalPrefixFor_155"]/tr')
    if len(curList) > 0:
        # v2 Detalles del Procedimiento
        output['_CONDITIONAL_PREFIX_FOR'] = 155
        assert 'NUMERO_PROCEDIMIENTO' not in output
        assert curList[0].xpath('th/h4')[0].text == 'DATOS GENERALES DEL PROCEDIMIENTO DE CONTRATACIÓN'
        assert curList[1].attrib['class'] == 'accessHidden'
        curList1 = curList[2:6]
        curSpec = [(var, '\n' + label) for (var, label) in
            [('NUMERO_PROCEDIMIENTO', 'Número del Procedimiento (Expediente)'),
             ('CARACTER', 'Carácter del procedimiento'),
             ('FORMA_PROCEDIMIENTO', 'Medio o forma del procedimiento'),
             ('_EXCLUSIVO_MIPYMES', 'Procedimiento exclusivo para MIPYMES')
            ]
          ]
        output.update(extractTable(curList1, curSpec, labelIdx=1, valIdx=3))
        # v2 Anexos
        assert curList[6].xpath('th/h4')[0].text == 'ANEXOS DEL PROCEDIMIENTO DE CONTRATACIÓN'
        assert curList[7].attrib['class'] == 'accessHidden'
        archives = []
        for row in curList[8:]:
            links = row[3].xpath('span/a')
            if len(links) > 0:
                assert len(links) == 1
                link = links[0]
                archive = {}
                archive['href'] = link.attrib['href']
                archive['nombre'] = link.attrib['title'].replace('Descargar archivo adjunto: ','')
                archive['tamano'] = re.match(r'.+\(([0-9,]+\s.B)\)', row[3][0].text_content()).group(1)
                archive['descripcion'] = row[1].text
                archive['comentarios'] = row[2][0].text
                archives.append(archive)
        # add conditionalPrefix archives
        output['_ANEXOS_CP'] = archives

    # Opportunity detail -- includes Procedimiento, Responsable, and possibly the Anexos sections
    oppDetail = cntDetail.xpath('form[@name="opportunityDetailForm"]/div')[0]

    # Procedimiento section
    curList = oppDetail.xpath('h3[normalize-space(text())="Procedimiento"]/following-sibling::table[1]/tr')
    # this section is not always present
    if len(curList) > 0:
        assert curList[0][2].text == 'Código'
        assert curList[0][3].text == 'Título'
        assert curList[0][4].text == 'FECHA Y HORA LÍMITE PARA PRESENTAR PROPOSICIONES'
        procedimientos = []
        for i in range(1, len(curList)):
            procedimientos.append(
                {'codigo_procedimiento': curList[i][2].text,
                 'titulo_procedimiento': curList[i][3].text,
                 'fecha_limite_procedimiento': curList[1][4].text
                }
            )
        output['_PROCEDIMIENTOS'] = procedimientos

    # Responsable section
    curList = oppDetail.xpath('h3[contains(text(),"Responsable")]/following-sibling::div[1]/ul/li')
    curSpec = [('NOMBRE_DE_LA_UC','Nombre de la Unidad Compradora (UC)'),
               ('_NOMBRE_DEL_OPERADOR','Nombre del Operador en la UC'),
               ('_CORREO_DEL_OPERADOR','Correo Electrónico del Operador en la UC')
              ]
    output.update(extractTable(curList, curSpec))

    # Anexos section
    curList = oppDetail.xpath('h3[contains(text(),"Anexos")]/following-sibling::table[1]/tr')
    # this section is not always present
    if len(curList) > 0:
        archives = []

        assert curList[0][1].text == 'Nombre del archivo'
        assert curList[0][2].text == 'Descripción del archivo'
        assert curList[0][3].text == 'Comentarios sobre Anexos'
        assert curList[0][4].text == 'Ultima fecha de modificación'

        for row in curList[1:]:
            archive = {}
            archive['href'] = row[1][1].attrib['href']
            archive['nombre'] = row[1][1].attrib['title'].replace('Descargar archivo adjunto: ','')
            archive['tamano'] = re.match(r'.+\(([0-9,]+ .B)\)', row[1].text_content()).group(1)
            archive['descripcion'] = row[2].text
            archive['comentarios'] = row[3].text
            archive['fecha_de_modificacion'] = row[4].text
            archives.append(archive)

        # add opportunityDetail archives
        output['_ANEXOS_OD'] = archives

    return(output)


def scrapeURL(url):
    # check that URL is not null
    if pd.isnull(url):
        return({})
    else:
        # get page and check status for error
        page = requests.get(url)
        page.raise_for_status()
        output = scrapePage(page.content)
        return(output)


def scrapeURL_wrapper(args):
    index, url, queue, halt, error, terminate = args
    if not halt.is_set():
        try:
            requested.add(index)
            output = scrapeURL(url)
            queue.put((index, output))
        except Exception as e:
            halt.set()
            error.set((e, traceback.format_exc()))
            terminate.set()


df_cont14 = pd.read_excel("data/raw/Contratos2014.xlsx").assign(year='2014')

outputs = {}
requested = set()
dataframe = df_cont14
n_processes = 4

while len(outputs) < len(dataframe):
    manager = Manager()
    pool = Pool(4)
    queue = manager.Queue()
    halt = manager.Event()
    terminate = manager.Event()
    error = manager.Value(tuple, None)

    indices = list(set(range(len(dataframe))).difference(set(outputs.keys())))
    result = pool.map_async(scrapeURL_wrapper,
                            [(index, dataframe['ANUNCIO'].iloc[index], queue, halt, error, terminate)
                             for index in indices])
                            #error_callback=callback)
    with tqdm(total=len(dataframe)) as pbar:
        while not result.ready():
            if terminate.is_set():
                pool.terminate()
                break
            if not queue.empty():
                index, value = queue.get()
                outputs[index] = value
                pbar.update()

    if error.get() is not None:
        e, format_exc = error.get()
        if isinstance(e, requests.exceptions.HTTPError) and e.response.status_code == 429:
            warnings.warn("Too many requests, pausing for 150 minutes...")
            for _ in trange(9059):
                time.sleep(1)
        else:
            print(format_exc, file=sys.stderr)
            break

output_df = pd.DataFrame(outputs[i] for i in range(len(dataframe)))
output_df.to_csv('data/interim/scraper_output14.csv', index=False)
