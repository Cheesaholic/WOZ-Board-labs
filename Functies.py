# Deze functie koppelt de BAG- en WOZ_NUM-tabellen en geeft een ruimtelijk object terug
def checkWOZNUM(gemeente, gemeenten):
    import geopandas as gpd
    from Functies import SDOtoShapely, getCentroids
    from threading import Thread
    from queue import Queue
    plaats = gemeenten.loc[gemeenten['GEMEENTENAAM'] == gemeente].reset_index()['GEMEENTECODE'][0]

    # Hier volgt het aanmaken van de threads voor het aanroepen van de BAG- en WOZ-tabellen
    bagqu = Queue()
    wozqu = Queue()
    wozt = Thread(target=woznumThread, args=(gemeente, plaats, wozqu))
    bagt = Thread(target=bagThread, args=(gemeente, plaats, bagqu))
    wozt.start()
    bagt.start()
    bagt.join()
    bag = bagqu.get()
    # Checkt of BAG query resultaten teruggeeft
    if bag.shape[0] > 0:
        wozt.join()
        woznum = wozqu.get()
        # Checkt of WOZ_NUM query resultaten teruggeeft
        if woznum.shape[0] > 0:
            print('Geometrie omzetten...')
            bag['geometry'] = [SDOtoShapely(g) for g in bag['GEOMETRY']]
            bag = bag.drop(labels=['GEOMETRY'], axis=1)

            bag['B_NUMMERAANDUIDING'] = bag['B_NUMMERAANDUIDING'].astype(int)
            
            wozlen = len(woznum)
            
            # DataFrames samenvoegen
            wozm = woznum.merge(bag, how='left', left_on='BAGNUMIDENTIFICATIE', right_on='B_NUMMERAANDUIDING')
            
#             wozm.sort_values(['B_OPENBARERUIMTE', 'HUISNUMMER', 'HUISLETTER', 'HUISNUMMERTOEVOEGING'], inplace=True)
#             wozm = wozm.reset_index()

            # Regels die niet gekoppeld kunnen worden apart opslaan
            wozfout = wozm.loc[wozm['B_NUMMERAANDUIDING'].isnull()]
            wozfout = wozfout[['BAGNUMIDENTIFICATIE', 'OPENBARERUIMTENAAM', 'POSTCODE', 'HUISNUMMER', 'HUISLETTER', 'HUISNUMMERTOEVOEGING', 'WOONPLAATSNAAM']]
            
            wozm['B_NUMMERAANDUIDING'] = wozm['B_NUMMERAANDUIDING'].fillna(value=0).astype(int)
            wozm = wozm.loc[wozm['B_NUMMERAANDUIDING'] != 0]
            wozm = wozm[['BAGNUMIDENTIFICATIE', 'B_NUMMERAANDUIDING', 'OPENBARERUIMTENAAM', 'B_OPENBARERUIMTE', 'POSTCODE', 'B_POSTCODE', 'HUISNUMMER', 'B_HUISNUMMER', 'HUISLETTER', 'B_HUISLETTER', 'HUISNUMMERTOEVOEGING', 'B_HUISNUMMERTOEVOEGING', 'WOONPLAATSNAAM', 'B_WOONPLAATSNAAM', 'geometry']]
            wozm = gpd.GeoDataFrame(wozm)
            wozm = getCentroids(wozm)
            wozm.crs = {'init' : 'epsg:28992'}
           
            print('Klaar')
            return bag, wozm, wozfout, wozlen
        else:
            raise KeyError('No WOZNUM-location with the locality-name ' + plaats + ' found')
    else:
        raise KeyError('No BAG-location with the locality-name ' + plaats + ' found')


    
# Thread voor het uitvoeren van de query uit de BAG
def bagThread(gemeente, plaats, queue):
    from pandas import read_sql
    from cx_Oracle import connect
    from yaml import load
    fname = "/home/jovyan/work/Data/woz-credentials.yaml"

    stream = open(fname, 'r')
    data = load(stream)

    ora_woz = connect(data['wozdb']['username']+'/'+data['wozdb']['password']+'@'+data['wozdb']['url'])
    ora_akw = connect(data['gmadb']['username']+'/'+data['gmadb']['password']+'@'+data['gmadb']['url'])
    
    query= """
    SELECT
      /*+leading(wpc woo ope num vbo) index(wpc IN_12404_WPCV) index(woo UK_01101_CWPLV_BGA) index(ope IN_02106_CORMV_BGA) index(num IN_03107_CNADV_BGA) index(vbo FK_05103_CNADI_CADOV_BGA_FK) */
      NUM.CNADV_CNADI_NUMMERAANDD_ID  AS b_Nummeraanduiding,
      WOO.CWPLV_NAAM                  AS b_Woonplaatsnaam,
      OPE.CORMV_NAAM                  AS b_Openbareruimte,
      NUM.CNADV_POSTCODE              AS b_Postcode,
      NUM.CNADV_HUISNUMMER            AS b_Huisnummer,
      NUM.CNADV_HUISLETTER            AS b_Huisletter,
      NUM.CNADV_HUISNUMMER_TOEVOEGING AS b_HuisnummerToevoeging,
      VBO.CADOV_GEOMETRIE             AS geometry
    FROM
      BGAP_OWN.CBGA_WOONPLAATSCODE_V WPC
      JOIN BGAP_OWN.CBGA_WOONPLAATS_V WOO ON WPC.WPCV_CWPLI_WOONPLAATS_ID             = WOO.CWPLV_CWPLI_WOONPLAATS_ID 
      JOIN BGAP_OWN.CBGA_OPENBARE_RUIMTE_V OPE ON WOO.CWPLV_CWPLI_WOONPLAATS_ID       = OPE.CORMV_CWPLI_WOONPLAATS_ID
      JOIN BGAP_OWN.CBGA_NUMMERAANDUIDING_V NUM ON OPE.CORMV_CORMI_OPENBARE_RUIMTE_ID = NUM.CNADV_CORMI_OPENBARE_RUIMTE_ID
      JOIN BGAP_OWN.CBGA_ADRESSEERBAAR_OBJECT_V VBO ON NUM.CNADV_CNADI_NUMMERAANDD_ID = VBO.CADOV_CNADI_NUMMERAANDD_ID
    WHERE
      WPC.WPCV_GMTI_GEMEENTECODE    = :1
      AND WPC.WPCV_GELDIGVAN        < CURRENT_DATE
      AND WPC.WPCV_GELDIGTOT        > CURRENT_DATE
      AND WOO.CWPLV_DT_GELDIG_VAN   < CURRENT_DATE
      AND WOO.CWPLV_DT_GELDIG_TOT   > CURRENT_DATE
      AND WOO.CWPLV_IND_NIET_ACTIEF = 0
      AND OPE.CORMV_DT_GELDIG_VAN   < CURRENT_DATE
      AND OPE.CORMV_DT_GELDIG_TOT   > CURRENT_DATE
      AND OPE.CORMV_IND_NIET_ACTIEF = 0
      AND NUM.CNADV_DT_GELDIG_VAN   < CURRENT_DATE
      AND NUM.CNADV_DT_GELDIG_TOT   > CURRENT_DATE
      AND NUM.CNADV_IND_NIET_ACTIEF = 0
      AND VBO.CADOV_DT_GELDIG_VAN   < CURRENT_DATE
      AND VBO.CADOV_DT_GELDIG_TOT   > CURRENT_DATE
      AND VBO.CADOV_IND_NIET_ACTIEF = 0
    """
    print('BAG query uitvoeren ' + gemeente + '...')
    bag = read_sql(query, con=ora_akw, params={plaats})
    queue.put(bag)
    print('BAG query uitgevoerd ' + gemeente + '...')

# Thread voor het uitvoeren van de query uit de WOZ_NUM tabel
def woznumThread(gemeente, plaats, queue):
    from pandas import read_sql
    from cx_Oracle import connect
    from yaml import load
    fname = "/home/jovyan/work/Data/woz-credentials.yaml"

    stream = open(fname, 'r')
    data = load(stream)

    ora_woz = connect(data['wozdb']['username']+'/'+data['wozdb']['password']+'@'+data['wozdb']['url'])
    ora_akw = connect(data['gmadb']['username']+'/'+data['gmadb']['password']+'@'+data['gmadb']['url'])
    
    query = """SELECT
        /*+leading(woz woznum num) index(woz IN_WOZ_SORT_C) index(woznum IN_WOZNUM_ACTUEEL_VIEW) index(num PK_NUM)  */
        NUM.bagnumidentificatie,
        NUM.woonplaatsnaam,
        NUM.openbareruimtenaam,
        NUM.postcode,
        NUM.huisnummer,
        NUM.huisletter,
        NUM.huisnummertoevoeging
    FROM
        wdo_woz WOZ 
        JOIN wdo_woznum WOZNUM ON WOZ.woz_historie_id = WOZNUM.woz_historie_id  
        JOIN wdo_num NUM       ON WOZNUM.num_id       = NUM.num_id 
    WHERE
        WOZ.VERANTWOORDELIJKEGEMEENTE = :1
        AND WOZ.eindregistratie IS NULL
        AND WOZ.eindgeldigheid IS NULL
        AND WOZNUM.eindregistratie IS NULL
        AND WOZNUM.eindgeldigheid IS NULL
        AND WOZNUM.EINDRELATIE IS NULL
    """
    
    print('WOZNUM query uitvoeren ' + gemeente + '...')
    woznum = read_sql(query, con=ora_woz, params={int(plaats)})
    queue.put(woznum)    
    print('WOZNUM query uitgevoerd ' + gemeente + '...')

# Alle ruimtelijke objecten omzetten in punten, in plaats van geometrieën
def getCentroids(gdf):
    import geopandas as gpd
    if type(gdf) == gpd.geodataframe.GeoDataFrame:
        if(gdf.geometry.notnull().all()):
            gdf.geometry = gdf.geometry.centroid
        else:
            gdf.geometry.loc[gdf.geometry.notnull()] = gdf.geometry.loc[gdf.geometry.notnull()].centroid
    else:
        raise ValueError('No GeoDataFrame found')
    return gdf


# Zet een DataFrame om in een JSON object
def gdfToJson(gdf, attr, name, fields, aliases):
    json = folium.GeoJson(
        gdf.loc[gdf['geometry'].notnull()][attr].to_crs({'init': 'epsg:4326'}).to_json(),
        name=name,
        show=True,
        style_function=lambda feature: {
            'fillColor': 'green',
            'color': 'black',
            'weight': 1,
            'dashArray': '5, 5',
            'fillOpacity': 0.5
        },
        highlight_function=lambda x: {'weight': 3,
                                      'color': 'black',
                                      'fillOpacity': 1},
        tooltip=folium.features.GeoJsonTooltip(
            fields=fields,
            aliases=aliases
        ),
    )
    return json

# Deze functie maakt de kolommen in de tabel rood als ze verkeerd zijn
def Template(column):
    if(column == 'openbareruimte'):
        col = ['OPENBARERUIMTENAAM', 'B_OPENBARERUIMTE']
    elif(column == 'postcode'):
        col = ['POSTCODE', 'B_POSTCODE']
    elif(column == 'huisnummer'):
        col = ['HUISNUMMER', 'B_HUISNUMMER']
    elif(column == 'huisletter'):
        col = ['HUISLETTER', 'B_HUISLETTER']
    elif(column == 'huisnummertoevoeging'):
        col = ['HUISNUMMERTOEVOEGING', 'B_HUISNUMMERTOEVOEGING']
    style = """                
            <div style="color: <%= 
                    (function colorfromint(){
                        if(""" + col[0] + """ != """ + col[1] + """){return('red')}
                        }()) %>;"> 
                <%= value %>
                </font>
            </div>
            """
    return style

# Standaardtemplate voor spacing
def BlackTemplate():
    style = """                
            <div> 
                <%= value %>
                </font>
            </div>
            """
    return style
        
        
    

# Zet het Rijksdriehoeksstelsel-geometrieënstelsel om in de standaard voor kaarten (WGS84)
def RD2GPS(gdf):
    import geopandas as gpd
    if type(gdf) == gpd.geodataframe.GeoDataFrame:
        if(gdf.geometry.notnull().all()):
            gdf = gdf.to_crs({'init': 'epsg:4326'})
            gdf['x'] = gdf.geometry.centroid.x
            gdf['y'] = gdf.geometry.centroid.y
        else:
            gdf.loc[gdf.geometry.notnull()] = gdf.loc[gdf.geometry.notnull()].to_crs({'init': 'epsg:4326'})
            gdf.crs = {'init': 'epsg:4326'}
            gdf['x'] = ''
            gdf['y'] = ''
            gdf['x'].loc[gdf.geometry.notnull()] = gdf.loc[gdf.geometry.notnull()].centroid.x
            gdf['y'].loc[gdf.geometry.notnull()] = gdf.loc[gdf.geometry.notnull()].centroid.y
    else:
        raise ValueError('No GeoDataFrame found')
    return gdf


# Zet het Rijksdriehoeksstelsel-geometrieënstelsel om in de standaard voor Bokeh (Mercator)
def RD2Merc(gdf):
    import geopandas as gpd
    if type(gdf) == gpd.geodataframe.GeoDataFrame:
        if(gdf.geometry.notnull().all()):
            gdf = gdf.to_crs({'init': 'epsg:3857'})
            gdf['x'] = gdf.geometry.centroid.x
            gdf['y'] = gdf.geometry.centroid.y
        else:
            gdf.loc[gdf.geometry.notnull()] = gdf.loc[gdf.geometry.notnull()].to_crs({'init': 'epsg:3857'})
            gdf.crs = {'init': 'epsg:3857'}
            gdf['x'] = ''
            gdf['y'] = ''
            gdf['x'].loc[gdf.geometry.notnull()] = gdf.loc[gdf.geometry.notnull()].centroid.x
            gdf['y'].loc[gdf.geometry.notnull()] = gdf.loc[gdf.geometry.notnull()].centroid.y
    else:
        raise ValueError('No GeoDataFrame found')
    return gdf


# Houdt rekening met filters en haalt zo alle foutieve regels uit de DataFrame
def getDifBagWoz(wozm, capitalize):
    import pandas as pd
    import geopandas as gpd
    
    def checkboxCheck(col):
        co = col
        if(2 not in capitalize):
            co = co.str.replace(r"str\b", "straat", regex=True).str.replace(r"ln\b", "laan", regex=True).str.replace(" v ", " van ").str.replace(" d ", " de ").str.replace(r"Burg\b", "Burgemeester", regex=True).str.replace(r"Jhr\b", "Jonkheer", regex=True).str.replace(" vd ", " van der ", regex=True).str.replace("pln", "plein")
        if(1 not in capitalize):
            co = co.str.replace(".", "").str.replace(" ", "")
        if(0 not in capitalize):
            co = co.str.lower()
        return co
    
    if ((type(wozm) == gpd.geodataframe.GeoDataFrame) | (type(wozm) == pd.core.frame.DataFrame)):
        wozm = wozm.loc[(wozm['OPENBARERUIMTENAAM'] != wozm['B_OPENBARERUIMTE']) | (((wozm['POSTCODE'].notnull()) & (wozm['B_POSTCODE'].notnull())) & (wozm['POSTCODE'] != wozm['B_POSTCODE']))   | (wozm['HUISNUMMER'] != wozm['B_HUISNUMMER']) | (((wozm['HUISLETTER'].notnull()) & (wozm['B_HUISLETTER'].notnull())) & (wozm['HUISLETTER'] != wozm['B_HUISLETTER'])) | (((wozm['HUISNUMMERTOEVOEGING'].notnull()) & (wozm['B_HUISNUMMERTOEVOEGING'].notnull())) & (wozm['HUISNUMMERTOEVOEGING'] != wozm['B_HUISNUMMERTOEVOEGING']))]
        if(capitalize != [1,2,3]):
            wozm = wozm.loc[(checkboxCheck(wozm['OPENBARERUIMTENAAM']) != checkboxCheck(wozm['B_OPENBARERUIMTE'])) | (((wozm['POSTCODE'].notnull()) & (wozm['B_POSTCODE'].notnull())) & (checkboxCheck(wozm['POSTCODE']) != checkboxCheck(wozm['B_POSTCODE'])))   | (wozm['HUISNUMMER'] != wozm['B_HUISNUMMER']) | (((wozm['HUISLETTER'].notnull()) & (wozm['B_HUISLETTER'].notnull())) & (checkboxCheck(wozm['HUISLETTER']) != checkboxCheck(wozm['B_HUISLETTER']))) | (((wozm['HUISNUMMERTOEVOEGING'].notnull()) & (wozm['B_HUISNUMMERTOEVOEGING'].notnull())) & (checkboxCheck(wozm['HUISNUMMERTOEVOEGING']) != checkboxCheck(wozm['B_HUISNUMMERTOEVOEGING'])))]
    else:
        raise ValueError('No GeoDataFrame or DataFrame found')
    return wozm


# Zet een Pandas DataFrame om in een Bokeh tabel-object
def dfToCDS(df, datatype, width=1000, height=280):
    import pandas as pd
    import geopandas as gpd
    from bokeh.models import ColumnDataSource
    from bokeh.models.widgets import TableColumn, DataTable
    if ((type(df) == gpd.geodataframe.GeoDataFrame) | (type(df) == pd.core.frame.DataFrame)):
        df['BAGNUMIDENTIFICATIE'] = df['BAGNUMIDENTIFICATIE'].apply(lambda x: '0'+str(x))
        df.index += 1
        df.sort_values(by=['OPENBARERUIMTENAAM', 'HUISNUMMER', 'HUISLETTER', 'HUISNUMMERTOEVOEGING'])
        if(datatype == 'WOZNUMDif'):
#             df.sort_values('OPENBARERUIMTENAAM')
            data = ColumnDataSource(dict(df[['BAGNUMIDENTIFICATIE',
             'B_NUMMERAANDUIDING',
             'OPENBARERUIMTENAAM',
             'B_OPENBARERUIMTE',
             'POSTCODE',
             'B_POSTCODE',
             'HUISNUMMER',
             'B_HUISNUMMER',
             'HUISLETTER',
             'B_HUISLETTER',
             'HUISNUMMERTOEVOEGING',
             'B_HUISNUMMERTOEVOEGING',
             'WOONPLAATSNAAM',
             'B_WOONPLAATSNAAM', 'x', 'y']]))
        elif(datatype == 'WOZNUMNULL'):
            data = ColumnDataSource(dict(df[['BAGNUMIDENTIFICATIE',
             'OPENBARERUIMTENAAM',
             'POSTCODE',
             'HUISNUMMER',
             'HUISLETTER',
             'HUISNUMMERTOEVOEGING',
             'NUMJUIST',
             'WOONPLAATSNAAM']]))
    else:
        raise ValueError('No GeoDataFrame or DataFrame found')
    return data


# Maakt achtergrondkaart aan voor kaart
def nlmaps():
    from bokeh.models.tiles import WMTSTileSource
    return WMTSTileSource(attribution="&copy; Copyright <a href='https://kadaster.nl/'>Kadaster</a>, Product by Marnix Ober. All rights reserved.", url="https://geodata.nationaalgeoregister.nl/tiles/service/wmts/brtachtergrondkaart/EPSG:3857/{Z}/{X}/{Y}.png")


# Zoekt met Reverse-lookup de juiste nummeraanduiding op, als deze niet gevonden kan worden in de BAG
def getRightNummeraanduiding(checkwoznum, wozfout):
    numloc = checkwoznum.loc[(checkwoznum['B_OPENBARERUIMTE'] == wozfout.OPENBARERUIMTENAAM) & (checkwoznum['B_POSTCODE'] == wozfout.POSTCODE) & (checkwoznum['B_HUISNUMMER'] == wozfout.HUISNUMMER) & ((checkwoznum['B_HUISLETTER'] == wozfout.HUISLETTER) | ((checkwoznum['B_HUISLETTER'].isnull()) & (wozfout.HUISLETTER is None))) & ((checkwoznum['B_HUISNUMMERTOEVOEGING'] == wozfout.HUISNUMMERTOEVOEGING) | ((checkwoznum['B_HUISNUMMERTOEVOEGING'].isnull()) & (wozfout.HUISNUMMERTOEVOEGING is None)))]
    if(len(numloc) > 0):
        if(len(numloc) < 2):
            return '0' + str(numloc.reset_index(drop=True)['B_NUMMERAANDUIDING'][0])
        else:
            return 'Meer dan een nummeraanduiding'
    else:
        return 'Niet gevonden'

# Haalt bij het begin van het dashboard alle woonplaatsen op uit de BAG
def getAllWoonplaats():
    from pandas import read_sql
    from cx_Oracle import connect
    from yaml import load
    fname = "/home/jovyan/work/Data/woz-credentials.yaml"

    stream = open(fname, 'r')
    data = load(stream)

    ora_woz = connect(data['wozdb']['username']+'/'+data['wozdb']['password']+'@'+data['wozdb']['url'])
    ora_akw = connect(data['gmadb']['username']+'/'+data['gmadb']['password']+'@'+data['gmadb']['url'])
    
#     wooq = """
#     SELECT DISTINCT
#       /*+leading(woo) index(woo UK_01101_CWPLV_BGA)*/
#       WOO.CWPLV_NAAM  AS woonplaats
#     FROM
#       BGAP_OWN.CBGA_WOONPLAATS_V WOO
#     WHERE
#           WOO.CWPLV_DT_GELDIG_VAN < CURRENT_DATE
#       AND WOO.CWPLV_DT_GELDIG_TOT > CURRENT_DATE
#       AND WOO.CWPLV_IND_NIET_ACTIEF   = 0
#     ORDER BY WOO.CWPLV_NAAM ASC
#     """

    wooq = """
    SELECT DISTINCT WOONPLAATS, GEMEENTECODE, GEMEENTENAAM
    FROM GMA_OWN.KS_WOONPLAATSEN
    WHERE DATUM_INGANG < CURRENT_DATE
    AND DATUM_EINDE IS NULL OR DATUM_EINDE > CURRENT_DATE
    ORDER BY GEMEENTENAAM ASC
    """
    
    woo = read_sql(wooq, con=ora_akw)
    
    wooSelect = ['Selecteer...'] + woo['GEMEENTENAAM'].drop_duplicates().tolist()
    
    return woo, wooSelect

# Maakt een laadicoon aan
def Loading():
    from bokeh.layouts import layout
    from bokeh.models.widgets import Div
    return layout([Div(text="""<img src="https://mir-s3-cdn-cf.behance.net/project_modules/disp/1f430a36197347.57135ca19bbf5.gif" alt="Dashboard is aan het laden...">""")])

# Zet 3D-geometrieën om in 2D-geometrieën
def remove_third_dimension(geom):
    if geom.is_empty:
        return geom

    if isinstance(geom, Polygon):
        exterior = geom.exterior
        new_exterior = remove_third_dimension(exterior)

        interiors = geom.interiors
        new_interiors = []
        for int in interiors:
            new_interiors.append(remove_third_dimension(int))

        return Polygon(new_exterior, new_interiors)

    elif isinstance(geom, LinearRing):
        return LinearRing([xy[0:2] for xy in list(geom.coords)])

    elif isinstance(geom, LineString):
        return LineString([xy[0:2] for xy in list(geom.coords)])

    elif isinstance(geom, Point):
        return Point([xy[0:2] for xy in list(geom.coords)])

    elif isinstance(geom, MultiPoint):
        points = list(geom.geoms)
        new_points = []
        for point in points:
            new_points.append(remove_third_dimension(point))

        return MultiPoint(new_points)

    elif isinstance(geom, MultiLineString):
        lines = list(geom.geoms)
        new_lines = []
        for line in lines:
            new_lines.append(remove_third_dimension(line))

        return MultiLineString(new_lines)

    elif isinstance(geom, MultiPolygon):
        pols = list(geom.geoms)

        new_pols = []
        for pol in pols:
            new_pols.append(remove_third_dimension(pol))

        return MultiPolygon(new_pols)

    elif isinstance(geom, GeometryCollection):
        geoms = list(geom.geoms)

        new_geoms = []
        for geom in geoms:
            new_geoms.append(remove_third_dimension(geom))

        return GeometryCollection(new_geoms)

    else:
        raise RuntimeError("Currently this type of geometry is not supported: {}".format(type(geom)))

# Gebruikt JavaScript om de (geselecteerde) regels terug te geven als CSV bestand       
def csvButtonJS(plaats, soort, full=True):
    if(soort == 'WOZNUMNULL'):
        text = 'export_onjuiste_nummeraanduidingen_'
    elif(soort == 'WOZNUMDif'):
        text = 'export_verkeerde_informatie_'
    else:
        raise ValueError('Onjuiste soort ' + soort)
    if(full == True):
        js = """
        function table_to_csv(source) {
            const columns = Object.keys(s1.data)
            const nrows = s1.get_length()
            const lines = [columns.join(',')]

            for (let i = 0; i < nrows; i++) {
                let row = [];
                for (let j = 0; j < columns.length; j++) {
                    const column = columns[j]
                    row.push(s1.data[column][i].toString())
                }
                lines.push(row.join(','))
            }
            return lines.join('\\n').concat('\\n')
        }


        const filename = '""" + text + plaats + """.csv'
        filetext = table_to_csv(s1)
        const blob = new Blob([filetext], { type: 'text/csv;charset=utf-8;' })

        //addresses IE
        if (navigator.msSaveBlob) {
            navigator.msSaveBlob(blob, filename)
        } else {
            const link = document.createElement('a')
            link.href = URL.createObjectURL(blob)
            link.download = filename
            link.target = '_blank'
            link.style.visibility = 'hidden'
            link.dispatchEvent(new MouseEvent('click'))
        }
        """
    else:
        js = """
        function table_to_csv(source) {
            const columns = Object.keys(s1.data)
            const lines = [columns.join(',')]
            const inds = s1.selected.indices

            for (let i = 0; i < inds.length; i++) {
                let row = [];
                for (let j = 0; j < columns.length; j++) {
                    const ind = inds[i]
                    const column = columns[j]
                    row.push(s1.data[column][ind].toString())
                }
                lines.push(row.join(','))
            }
            return lines.join('\\n').concat('\\n')
        }


        const filename = '""" + text + plaats + """.csv'
        filetext = table_to_csv(s1)
        const blob = new Blob([filetext], { type: 'text/csv;charset=utf-8;' })

        //addresses IE
        if (navigator.msSaveBlob) {
            navigator.msSaveBlob(blob, filename)
        } else {
            const link = document.createElement('a')
            link.href = URL.createObjectURL(blob)
            link.download = filename
            link.target = '_blank'
            link.style.visibility = 'hidden'
            link.dispatchEvent(new MouseEvent('click'))
        }
        """
    return js

# Maakt tabel aan voor dashboard
def getTable(tablename, s1, s_with=100):
    from bokeh.models.widgets import TableColumn, HTMLTemplateFormatter, DataTable
    from bokeh.models.widgets.tables import DateFormatter
    from bokeh.models.sources import CDSView
    from bokeh.models.filters import IndexFilter
    if(tablename == 'WOZNUMDif'):
        columns = [
        TableColumn(field='WOONPLAATSNAAM',
                    title='Woonplaats', formatter=HTMLTemplateFormatter(template=BlackTemplate())),
        TableColumn(field='BAGNUMIDENTIFICATIE',
                    title='Nummeraanduiding', formatter=HTMLTemplateFormatter(template=BlackTemplate())),
        TableColumn(field='OPENBARERUIMTENAAM', title='WOZ Openbare Ruimte'
                    ,
                    formatter=HTMLTemplateFormatter(template=Template('openbareruimte'
                    ))),
        TableColumn(field='B_OPENBARERUIMTE',
                    title='BAG Openbare Ruimte', formatter=HTMLTemplateFormatter(template=BlackTemplate())),
        TableColumn(field='POSTCODE', title='WOZ Postcode',
                    formatter=HTMLTemplateFormatter(template=Template('postcode'
                    )), width=150),
        TableColumn(field='B_POSTCODE', title='BAG Postcode',  width=150, formatter=HTMLTemplateFormatter(template=BlackTemplate())),
        TableColumn(field='HUISNUMMER', title='WOZ Huisnummer',
                    formatter=HTMLTemplateFormatter(template=Template('huisnummer'
                    )), width=s_with),
        TableColumn(field='B_HUISNUMMER', title='BAG Huisnummer', width=s_with, formatter=HTMLTemplateFormatter(template=BlackTemplate())),
        TableColumn(field='HUISLETTER', title='WOZ Huisletter',
                    formatter=HTMLTemplateFormatter(template=Template('huisletter'
                    )), width=s_with),
        TableColumn(field='B_HUISLETTER', title='BAG Huisletter', width=s_with, formatter=HTMLTemplateFormatter(template=BlackTemplate())),
        TableColumn(field='HUISNUMMERTOEVOEGING',
                    title='WOZ Huisnummertoevoeging',
                    formatter=HTMLTemplateFormatter(template=Template('huisnummertoevoeging'
                    )), width=s_with),
        TableColumn(field='B_HUISNUMMERTOEVOEGING',
                    title='BAG Huisnummertoevoeging', width=s_with, formatter=HTMLTemplateFormatter(template=BlackTemplate())),
        ]
        table = DataTable(source=s1, columns=columns, width=1200,
                      height=600)
        return table
    elif(tablename == 'WOZNUMNULL'):
        columns = [
            TableColumn(field="WOONPLAATSNAAM", title='Woonplaats'),
            TableColumn(field="BAGNUMIDENTIFICATIE", title="WOZ Nummeraanduiding"),
            TableColumn(field="OPENBARERUIMTENAAM", title="WOZ Openbare Ruimte"),
            TableColumn(field="POSTCODE", title="WOZ Postcode"),
            TableColumn(field="HUISNUMMER", title="WOZ Huisnummer"),
            TableColumn(field="HUISLETTER", title="WOZ Huisletter"),
            TableColumn(field="HUISNUMMERTOEVOEGING", title="WOZ Huisnummertoevoeging"),
            TableColumn(field="NUMJUIST", title="Juiste Nummeraanduiding")
        ]
        table = DataTable(source=s1, columns=columns, width=1000,
                      height=600, sizing_mode='fixed')
        return table
    elif(tablename == 'Leaderboard'):
        columns = [
            TableColumn(field="plaats", title='Plaats'),
            TableColumn(field="score", title="Score (%)"),
            TableColumn(field="gemnaam", title="Gemeentenaam"),
            TableColumn(field="diflen", title="Aantal verkeerde informatie"),
            TableColumn(field="foutlen", title="Aantal foutieve BAG-waarden"),
            TableColumn(field="wozlen", title="Aantal WOZ-objecten"),
            TableColumn(field="datetime", title="Laatst geüpdatet")
        ]
        table = DataTable(source=s1, columns=columns, width=700,
                      height=600)
        return table

# Zet Oracle geometrie om in Shapely geometrie
def SDOtoShapely(g):
    if (g.SDO_ORDINATES is not None) or (g.SDO_POINT is not None):
        if g.SDO_GTYPE == 3003:
            from shapely.geometry import Polygon
            return Polygon(list(zip(*[iter(g.SDO_ORDINATES.aslist())] * 3)))
        elif g.SDO_GTYPE == 2003:
            from shapely.geometry import Polygon
            return Polygon(list(zip(*[iter(g.SDO_ORDINATES.aslist())] * 2)))
        elif g.SDO_GTYPE == 3001:
            from shapely.geometry import Point
            return Point(g.SDO_POINT.X, g.SDO_POINT.Y, g.SDO_POINT.Z)
        elif g.SDO_GTYPE == 2001:
            from shapely.geometry import Point
            return Point(g.SDO_POINT.X, g.SDO_POINT.Y)
        else:
            return g
    else:
        return g

    
# def getGemeenteGeo():
#     from pandas import read_sql
#     from geopandas import GeoDataFrame
#     from cx_Oracle import connect
#     from yaml import load
#     import shapely.wkt
#     fname = "/home/jovyan/work/Data/woz-credentials.yaml"

#     stream = open(fname, 'r')
#     data = load(stream)

#     ora_woz = connect(data['wozdb']['username']+'/'+data['wozdb']['password']+'@'+data['wozdb']['url'])
#     ora_akw = connect(data['gmadb']['username']+'/'+data['gmadb']['password']+'@'+data['gmadb']['url'])
    
#     query = """
#     SELECT GM_CODE, SDO_UTIL.TO_WKTGEOMETRY(GEOMETRIE) AS geometry
#     FROM GMA_OWN.CBS_GEMEENTE
#     WHERE WATER = 'NEE'
#     """
    
#     gemgeo = read_sql(query, con=ora_akw)
    
#     gemgeo['GEOMETRY'] = [shapely.wkt.loads(g.read()) for g in gemgeo['GEOMETRY']]
#     gemgeo['GM_CODE'] = gemgeo['GM_CODE'].str.replace('GM', '')
#     gemgeo.columns = ['gemcode', 'geometry']
#     gemgeo = GeoDataFrame(gemgeo)
#     gemgeo.crs = {'init' : 'epsg:28992'}
#     gemgeo = gemgeo.to_crs({'init': 'epsg:3857'})
    
#     return gemgeo

def getGemeenteGeo(g):
    
    from pandas import read_sql
    from geopandas import GeoDataFrame
    from cx_Oracle import connect
    from yaml import load
    import shapely.wkt
    fname = "/home/jovyan/work/Data/woz-credentials.yaml"

    stream = open(fname, 'r')
    data = load(stream)

    ora_woz = connect(data['wozdb']['username']+'/'+data['wozdb']['password']+'@'+data['wozdb']['url'])
    ora_akw = connect(data['gmadb']['username']+'/'+data['gmadb']['password']+'@'+data['gmadb']['url'])
    g = str(g)
    if(len(g) == 3):
        g = '0' + g
    elif(len(g) == 2):
        g = '00' + g
    elif(len(g) == 1):
        g = '000' + g
    query = """
    /*+leading(wpc wpv) index(wpc IN_12404_WPCV) index(wpv UK_01101_CWPLV_BGA) */
    select SDO_UTIL.TO_WKTGEOMETRY(SDO_AGGR_UNION(SDOAGGRTYPE(wpv.CWPLV_GEOMETRIE, 0.05))) geom 
    from BGAP_OWN.CBGA_WOONPLAATS_V wpv
    JOIN BGAP_OWN.CBGA_WOONPLAATSCODE_V wpc ON wpv.CWPLV_CWPLI_WOONPLAATS_ID = wpc.WPCV_CWPLI_WOONPLAATS_ID
    where WPC.WPCV_GMTI_GEMEENTECODE = :1
      AND WPC.WPCV_GELDIGVAN        < CURRENT_DATE
      AND WPC.WPCV_GELDIGTOT        > CURRENT_DATE
      AND WPV.CWPLV_DT_GELDIG_VAN   < CURRENT_DATE
      AND WPV.CWPLV_DT_GELDIG_TOT   > CURRENT_DATE
      AND WPV.CWPLV_IND_NIET_ACTIEF = 0
    """
    
    try:
        gemgeo = read_sql(query, con=ora_akw, params={g})
        
    except:
        query = """
        SELECT SDO_UTIL.TO_WKTGEOMETRY(GEOMETRIE) AS GEOM
        FROM GMA_OWN.CBS_GEMEENTE
        WHERE GM_CODE = :1
        AND WATER = 'NEE'
        """
        g2 = 'GM' + g
        gemgeo = read_sql(query, con=ora_akw, params={g2})
    
    gemgeo = shapely.wkt.loads(gemgeo.loc[0, 'GEOM'].read())    
    return gemgeo

def getLeaderboard():
    from bokeh.models import GeoJSONDataSource, HoverTool, LinearColorMapper, ColorBar
    import Functies
    import pandas as pd
    import bokeh
    import geopandas as gpd
    import os
    from bokeh.palettes import brewer


    json_file = open(r"/home/jovyan/work/Marnix/woz-board/leaderboard/leader_json.txt", 'r')
    json_text = json_file.read()
    json_file.close()

    cds = GeoJSONDataSource(geojson=json_text)

    p = bokeh.plotting.figure(tools=['box_zoom', 'tap'
                                , 'wheel_zoom', 'pan','reset'], active_scroll='wheel_zoom', active_drag='pan', active_tap='tap')

    palette = brewer['RdYlGn'][11]
    palette = palette[::-1]
    color_mapper = LinearColorMapper(palette = palette, high=100, low=95)
    g = bokeh.models.glyphs.Patches(xs='xs', ys='ys', fill_color={'field': 'score', 'transform': color_mapper}, fill_alpha=0.6)
    color_bar = ColorBar(color_mapper=color_mapper, label_standoff=8,width = 500, height = 20,
                         border_line_color=None,location ='bottom_center', orientation = 'horizontal')
    p.add_layout(color_bar, 'below')
    p.add_tile(Functies.nlmaps())
    p.toolbar.logo = None
    p.axis.visible = False
    p.add_glyph(cds, g)
    p.add_tools(HoverTool(tooltips=[('Gemeentenaam',
                    '@gemnaam'
                    ), ('Plaats',
                    '#@plaats'
                    ), ('Score', 
                    '@score')]))

    table = Functies.getTable('Leaderboard', cds)

    return p, table
    
    
def geopandas2Bokeh(gdf):
    from bokeh.models import ColumnDataSource
    gdf_new = gdf.drop('geometry', axis=1).copy()
    gdf_new['x'] = gdf.apply(getGeometryCoords, 
                             geom='geometry', 
                             coord_type='x', 
                             shape_type='polygon', 
                             axis=1)
    
    gdf_new['y'] = gdf.apply(getGeometryCoords, 
                             geom='geometry', 
                             coord_type='y', 
                             shape_type='polygon', 
                             axis=1)
    
    return ColumnDataSource(gdf_new)


def getGeometryCoords(row, geom, coord_type, shape_type):
    
    # Parse the exterior of the coordinate
    if shape_type == 'polygon':
        exterior = row[geom].exterior
        if coord_type == 'x':
            # Get the x coordinates of the exterior
            return list( exterior.coords.xy[0] )    
        
        elif coord_type == 'y':
            # Get the y coordinates of the exterior
            return list( exterior.coords.xy[1] )

    elif shape_type == 'point':
        exterior = row[geom]
    
        if coord_type == 'x':
            # Get the x coordinates of the exterior
            return  exterior.coords.xy[0][0] 

        elif coord_type == 'y':
            # Get the y coordinates of the exterior
            return  exterior.coords.xy[1][0]    

        # TODO: Tabel WOZ implementatie        
def checkWOZ(plaats):
    print('woz')
#     query = """
#     SELECT
#     /*+leading(woz wrd woznum num) index(woz IN_WOZ_ACT2) index(wrd IN_WRD_WP_DB) index(woznum IN_WOZNUM_ACTUEEL_VIEW) index(num IN_NUM_BEVR_SEL2) */
#       NUM.BAGNUMIDENTIFICATIE,
#       WRD16.VASTGESTELDEWAARDE AS waarde16,
#       WRD17.VASTGESTELDEWAARDE AS waarde17,
#       WRD.VASTGESTELDEWAARDE AS waarde18
#     FROM
#       (
#         SELECT
#           WRDE17.VASTGESTELDEWAARDE,
#           WRDE17.WOZ_HISTORIE_ID,
#           WRDE17.EINDGELDIGHEID
#         FROM
#           WDO_WRD WRDE17
#         WHERE
#           WRDE17.WAARDEPEILDATUM >= '2017-00-00'
#           AND WRDE17.WAARDEPEILDATUM <= '2018-00-00'
#           AND WRDE17.EINDGELDIGHEID IS NULL
#       ) WRD17,
#       (
#         SELECT
#           WRDE16.VASTGESTELDEWAARDE,
#           WRDE16.WOZ_HISTORIE_ID,
#           WRDE16.EINDGELDIGHEID
#         FROM
#           WDO_WRD WRDE16
#         WHERE
#           WRDE16.WAARDEPEILDATUM >= '2016-00-00'
#           AND WRDE16.WAARDEPEILDATUM <= '2017-00-00'
#           AND WRDE16.EINDGELDIGHEID IS NULL
#       ) WRD16,
#       WDO_WOZ WOZ
#       JOIN WDO_WRD WRD ON       WOZ.WOZ_HISTORIE_ID = WRD.WOZ_HISTORIE_ID
#       JOIN WDO_WOZNUM WOZNUM ON WOZNUM.WOZ_HISTORIE_ID = WOZ.WOZ_HISTORIE_ID
#       JOIN WDO_NUM NUM ON       NUM.NUM_ID = WOZNUM.NUM_ID
#     WHERE
#       NUM.WOONPLAATSNAAM = 'Apeldoorn'
#       AND WOZ.EINDGELDIGHEID IS NULL
#       AND WOZ.EINDREGISTRATIE IS NULL
#       AND WRD.WAARDEPEILDATUM >= '2018-00-00'
#       AND WRD.EINDGELDIGHEID IS NULL
#       AND wrd.eindregistratie is null
#       AND WRD17.WOZ_HISTORIE_ID = WOZ.WOZ_HISTORIE_ID
#       AND WRD16.WOZ_HISTORIE_ID = WOZ.WOZ_HISTORIE_ID
#       AND WOZNUM.EINDGELDIGHEID IS NULL
#       AND WOZNUM.EINDREGISTRATIE IS NULL
#     """
    query = """SELECT
      /*+leading(num woznum woz wrd) index(num IN_NUM_BEVR_SEL2) index(woznum IN_WOZNUM_ACTUEEL_VIEW) index(woz IN_WOZ_ACT2) index(wrd IN_WRD_ACT)*/
      NUM.bagnumidentificatie,
      NUM.woonplaatsnaam,
      NUM.openbareruimtenaam,
      NUM.postcode,
      NUM.huisnummer,
      NUM.huisletter,
      NUM.huisnummertoevoeging,
      WRD16.VASTGESTELDEWAARDE AS waarde16,
      WRD17.VASTGESTELDEWAARDE AS waarde17,
      WRD.VASTGESTELDEWAARDE AS waarde18
    FROM
    (
        SELECT
        /*+leading(wrd) index(wrd IN_WRD_ACT)*/
          WRDE17.VASTGESTELDEWAARDE,
          WRDE17.WOZ_HISTORIE_ID,
          WRDE17.EINDGELDIGHEID,
          WRDE17.EINDREGISTRATIE
        FROM
          WDO_WRD WRDE17
        WHERE
          WRDE17.WOZ_HISTORIE_ID = WOZ.WOZ_HISTORIE_ID
          AND WRDE17.EINDGELDIGHEID IS NULL
          AND WRDE17.EINDREGISTRATIE IS NULL
          AND WRDE17.WAARDEPEILDATUM >= '2017-00-00'
          AND WRDE17.WAARDEPEILDATUM <= '2018-00-00'
      ) WRD17,
      (
        SELECT
        /*+leading(wrd) index(wrd IN_WRD_ACT)*/
          WRDE16.VASTGESTELDEWAARDE,
          WRDE16.WOZ_HISTORIE_ID,
          WRDE16.EINDGELDIGHEID,
          WRDE16.EINDREGISTRATIE
        FROM
          WDO_WRD WRDE16
        WHERE
          WRDE16.WOZ_HISTORIE_ID = WOZ.WOZ_HISTORIE_ID
          AND WRDE16.EINDGELDIGHEID IS NULL
          AND WRDE16.EINDREGISTRATIE IS NULL
          AND WRDE16.WAARDEPEILDATUM >= '2016-00-00'
          AND WRDE16.WAARDEPEILDATUM <= '2017-00-00'
      ) WRD16,
      wdo_num NUM
      JOIN wdo_woznum WOZNUM ON NUM.num_id = WOZNUM.num_id
      JOIN wdo_woz WOZ ON WOZNUM.woz_historie_id = WOZ.woz_historie_id
      JOIN WDO_WRD WRD ON WOZ.WOZ_HISTORIE_ID = WRD.WOZ_HISTORIE_ID
    WHERE
      woonplaatsnaam = :1
      AND WOZNUM.eindregistratie IS NULL
      AND WOZNUM.eindgeldigheid IS NULL
      AND WOZNUM.EINDRELATIE IS NULL
      AND WOZ.eindregistratie IS NULL
      AND WOZ.eindgeldigheid IS NULL
      AND WRD.WAARDEPEILDATUM >= '2018-00-00'
      AND WRD.EINDGELDIGHEID IS NULL
      AND wrd.eindregistratie is null
    """