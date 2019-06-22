#!/usr/bin/python
# -*- coding: utf-8 -*-
from Functies import checkWOZNUM, RD2GPS, SDOtoShapely, RD2Merc, \
    dfToCDS, nlmaps, getDifBagWoz, getAllWoonplaats
import Functies
import pandas as pd
import geopandas as gpd
import bokeh
from bokeh.plotting import curdoc
from bokeh.models import CustomJS, ColumnDataSource, HoverTool
from bokeh.models.widgets import TableColumn, DataTable, Div, \
    HTMLTemplateFormatter
from bokeh.models.widgets.groups import CheckboxGroup
from bokeh.events import ButtonClick
from flask import make_response, Flask, request, render_template, send_file, Response
from io import StringIO
from fontawesome.fontawesome_icon import FontAwesomeIcon
from functools import partial
import os
direc = os.getcwd()

# Aanmaken van een HTML document waar alle plots e.d. in geplaatst kunnen worden.
doc = curdoc()

# Functie die aangeroepen wordt bij het klikken op het dropdown-menu 'Tabel'
def update_woztable(wttr, old, new):
    if(new != 'Selecteer...'):
        if len(layout.children) > 2:
            layout.children = [layout.children[0], layout.children[1]]
        layoutnobut = bokeh.layouts.row(TableSelect)
        layout.children[1] = layoutnobut
        if(new == 'Nummeraanduidingen'):
            layout.children.append(loadinglong)
            gemcode = gemeenten.loc[gemeenten['GEMEENTENAAM'] == plaats.value].reset_index()['GEMEENTECODE'][0]
            global wozfout
            wozfout = pd.read_csv(str(direc) + '/work/Marnix/woz-board/Data/' + gemcode + '_f.csv')
            global dif
            dif = pd.read_csv(str(direc) + '/work/Marnix/woz-board/Data/' + gemcode + '_d.csv')
            dif = dif.fillna('')
            if loadinglong in layout.children:
                layout.children.remove(loadinglong)
            DifNULL.value = 'Selecteer...'
            DifBut = bokeh.models.widgets.buttons.Button(icon=FontAwesomeIcon(icon_name="question-circle", size=2),
                 label="BAG-waarden met verkeerde informatie", button_type='warning')
            NULLBut = bokeh.models.widgets.buttons.Button(icon=FontAwesomeIcon(icon_name="times-circle", size=2),
                 label="Onjuiste BAG-waarden", button_type='danger')
            DifBut.on_click(partial(DifNULLButton, val='BAG-waarden met verkeerde informatie'))
            NULLBut.on_click(partial(DifNULLButton, val='Onjuiste BAG-waarden'))
            layout1 = bokeh.layouts.layout([TableSelect, DifNULL], [DifBut, NULLBut])
            layout.children[1] = layout1
            
# Functie die aangeroepen wordt bij het klikken op het dropdown-menu 'Plaats'
def update_plaats(wttr, old, new):
    # Checkt of waarde niet 'Selecteer... is'
    if(new != 'Selecteer...'):
        # Haalt alle bestaande plots weg
        if len(layout.children) > 1:
            layout.children = [layout.children[0]]
        TableSelect.value = 'Selecteer...'
        NumBut = bokeh.models.widgets.buttons.Button(icon=FontAwesomeIcon(icon_name="map-marker", size=2),
             label="Nummeraanduidingen", button_type='primary')
        NumBut.on_click(partial(tableButton, val='Nummeraanduidingen'))
        layout1 = bokeh.layouts.layout([TableSelect],[NumBut])
        layout.children.append(layout1)
    else:
        # Als op 'Selecteer...' wordt geklikt, gaat het dashboard terug naar de originele staat
        layout.children = [layout.children[0]]

# Functie die aangeroepen wordt bij het klikken op het dropdown-menu 'Soort informatie'
def update_DifNULL(wttr, old, new):
    d_val = new
    # Checkt of waarde niet 'Selecteer... is'
    if(d_val != 'Selecteer...'):
        # Haalt alle bestaande plots weg
        if len(layout.children) > 2:
            layout.children = [layout.children[0], layout.children[1]]
        layout.children[1] = bokeh.layouts.row(TableSelect, DifNULL)
        # Voegt laad-afbeelding toe
        layout.children.append(loading)
        if(d_val == 'Onjuiste BAG-waarden'): 
            # Checkt of er meer dan één fout in de dataset zit.
            if len(wozfout) == 0:
                # Haalt laad-afbeelding weg
                if loading in layout.children:
                    layout.children.remove(loading)
                no_dif = \
                    bokeh.layouts.layout([bokeh.layouts.layout([Div(text="""<br><div class="alert alert-primary" role="alert">
  <h4 class="alert-heading">Geen onjuiste BAG-waarden gevonden</h4>
  <p>Er zijn geen onjuiste BAG-waarden in de WDO_NUM tabel gevonden voor de plaats """ + plaats.value + """.</p>
</div>""", width=620)])])
                # Voegt 'geen fouten gevonden' melding toe
                layoutno = bokeh.layouts.row(TableSelect, DifNULL)
                layout.children[1] = layoutno
                layout.children.append(no_dif)
            else:
                wozoj = wozfout
                # Maakt nieuwe kolom aan voor het zoeken van juiste nummeraanduidingen
#                 wozoj['NUMJUIST'] = [Functies.getRightNummeraanduiding(bag, g) for g in wozoj.itertuples()]
                # Vult NULL waarden
                wozoj = wozoj.fillna('')
                # Zet DataFrame om in Bokeh ColumnDataSource
                s1 = dfToCDS(wozoj, 'WOZNUMNULL')
                # Maakt een reactie op de csv knop
                csvButton.callback = CustomJS(args=dict(s1=s1), code=Functies.csvButtonJS(plaats.value, 'WOZNUMNULL'))
                # Maakt tabel
                table = Functies.getTable('WOZNUMNULL', s1)
                # Haalt laad-afbeelding weg
                if loading in layout.children:
                    layout.children.remove(loading)
                # Maakt layout
                layout1 = bokeh.layouts.row(TableSelect, DifNULL, csvButton)
                layout.children[1] = layout1
                layoutNULL = bokeh.layouts.layout(children=[table], sizing_mode="stretch_width")
                layout.children.append(layoutNULL)
        elif(d_val == 'BAG-waarden met verkeerde informatie'):
            # Roept functie aan om regels met verkeerde informatie terug te halen. (1,2,3 is voor de filters in het dashboard (hoofdlettergevoeligheid ed.))
#             dif = getDifBagWoz(checkwoznum, [1,2,3]).fillna('')
            # Geeft melding als er geen foute regels worden gevonden
            if len(dif) == 0:
                if loading in layout.children:
                    layout.children.remove(loading)
                    no_dif = \
                    bokeh.layouts.layout([bokeh.layouts.layout([Div(text="""<br><div class="alert alert-primary" role="alert">
  <h4 class="alert-heading">Geen verkeerde BAG-waarden gevonden</h4>
  <p>Er zijn geen BAG-waarden met verkeerde informatie in de WDO_NUM tabel gevonden voor de plaats """ + plaats.value + """.</p>
</div>""", width=640)])])
                layoutno = bokeh.layouts.row(TableSelect, DifNULL)
                layout.children[1] = layoutno
                layout.children.append(no_dif)
            else:
                capitalize.active = [0, 1, 2]
                Dif_plot_table(dif)
                
    else:
        # Als op 'Selecteer...' wordt geklikt, gaat het dashboard terug naar de originele staat
        if len(layout.children) > 2:
            layout.children = [layout.children[0], layout.children[1]]
        DifBut = bokeh.models.widgets.buttons.Button(icon=FontAwesomeIcon(icon_name="question-circle", size=2),
                 label="BAG-waarden met verkeerde informatie", button_type='warning')
        NULLBut = bokeh.models.widgets.buttons.Button(icon=FontAwesomeIcon(icon_name="times-circle", size=2),
                 label="Onjuiste BAG-waarden", button_type='danger')
        DifBut.on_click(partial(DifNULLButton, val='BAG-waarden met verkeerde informatie'))
        NULLBut.on_click(partial(DifNULLButton, val='Onjuiste BAG-waarden'))
        layout1 = bokeh.layouts.layout([TableSelect, DifNULL], [DifBut, NULLBut])
        layout.children[1] = layout1
        
# Functie die aangeroepen wordt als er een kaart en tabel gemaakt moeten worden
def Dif_plot_table(dif):
    # Zet om naar Mercatorsysteem
#     wn = RD2Merc(dif)
    wn = dif
    # Zet om naar Bokeh tabel
    s1 = dfToCDS(wn, 'WOZNUMDif')
    # Maak kaart-object aan
    p = bokeh.plotting.figure(tools=['lasso_select', 'reset', 'box_zoom'
                                , 'wheel_zoom', 'pan'], title=plaats.value, active_scroll='wheel_zoom', active_drag='pan')
    # Gebruik achtergrondkaart van het Kadaster
    p.add_tile(nlmaps())
    # Maak adrespunten aan
    p.circle('x', 'y', source=s1, size=7)
    # Voeg 'hovertool' toe om een muis-over object te maken
    p.add_tools(HoverTool(tooltips=[('WOZ adres',
                '@OPENBARERUIMTENAAM @HUISNUMMER @HUISNUMMERTOEVOEGING @HUISLETTER @POSTCODE'
                ), ('BAG adres',
                '@B_OPENBARERUIMTE @B_HUISNUMMER @B_HUISNUMMERTOEVOEGING @B_HUISLETTER @B_POSTCODE'
                ), ('Plaats', '@WOONPLAATSNAAM')]))
    # Geen Bokeh logo
    p.toolbar.logo = None
    # Geen assen
    p.axis.visible = False
    # Maak tabel aan
    table = Functies.getTable('WOZNUMDif', s1)
    # Maak csv-knoppen interactief
    csvButton.callback = CustomJS(args=dict(s1=s1), code=Functies.csvButtonJS(plaats.value, 'WOZNUMDif'))
    csvButtonSelect.callback = CustomJS(args=dict(s1=s1), code=Functies.csvButtonJS(plaats.value, 'WOZNUMDif', full=False))
    # Haal laad-icoon weg
    if loading in layout.children:
        layout.children.remove(loading)
    layout1 = bokeh.layouts.row(TableSelect, DifNULL, capitalize, csvButtonSelect, csvButton)
    layout.children[1] = layout1
    layout2 = bokeh.layouts.row(p, bokeh.layouts.column(children=[table], sizing_mode="stretch_width"))
    layout.children.append(layout2)
    # Haalt alle bestaande plots weg
    if len(layout.children) > 3:
            layout.children = [layout.children[0], layout.children[1], layout.children[2]]

# Updatet filters in tabel en plot
def update_capitalize(new):
    if len(layout.children) > 2:
        layout.children = [layout.children[0], layout.children[1]]
    diff = getDifBagWoz(dif, new)
    Dif_plot_table(diff)
#     print('Filters zijn niet operatief')

# Interactiviteit voor knoppen bij tabelkeuze
def tableButton(val):
    TableSelect.value = val
# Interactiviteit voor knoppen bij keuzes binnen WOZNUM-tabel
def DifNULLButton(val):
    DifNULL.value = val
    
def update_liveleader(attr, old, new):
    if((old == [0]) & (new == [0,1])):
        liveleader.active = [1]
        layoutleaderload = bokeh.layouts.layout([liveleader], [loading])
        layoutleader = bokeh.layouts.row(liveleader)
        layout.children = [layoutleaderload]
        p, table = Functies.getLeaderboard()
        layout.children = [layoutleader]
        layoutleaderboard = bokeh.layouts.row(children=[p, table],sizing_mode='stretch_both')
        layout.children.append(layoutleaderboard)
    elif((old == [1]) & (new == [0,1])):
        liveleader.active = [0]
        layout.children = [plaatst]

# Laadicoon initialiseren
loading = bokeh.layouts.layout([bokeh.layouts.layout([Div(text="""<img src="https://cdn.dribbble.com/users/63485/screenshots/2513799/untitled-5.gif" alt="Dashboard is aan het laden...">""")])])

# Laadicoon met tekst inititialiseren
loadinglong = bokeh.layouts.layout([bokeh.layouts.layout([Div(text="""<br><div class="alert alert-primary alert-dismissible fade show" role="alert">
    <h4 class="alert-heading">Een ogenblik geduld A.U.B.</h4>
     De data wordt opgehaald uit de database. Als er al een tijd geen queries meer zijn uitgevoerd kan dit tot 2 minuten duren bij grotere plaatsen.
    </div><img src="https://cdn.dribbble.com/users/63485/screenshots/2513799/untitled-5.gif" alt="Dashboard is aan het laden...">""", width=500)])])

# Tabel-select object initialiseren
TableSelect = bokeh.models.widgets.inputs.Select(options=['Selecteer...', 'Nummeraanduidingen'], title='Tabel')

# Tabel-select interactief maken
TableSelect.on_change('value', update_woztable)

# csv knop objecten initialiseren
csvButtonSelect = bokeh.models.widgets.buttons.Button(label='Download geselecteerde regels als csv', button_type="success", align='center',icon=FontAwesomeIcon(icon_name="download", size=1), css_classes=['downloadbut'])
csvButton = bokeh.models.widgets.buttons.Button(label='Download als csv', button_type="success", width=100, align='center',icon=FontAwesomeIcon(icon_name="download", size=1), css_classes=['downloadbut'])

# WOZNUM-select object initialiseren en interactief maken
DifNULL = bokeh.models.widgets.inputs.Select(options=['Selecteer...', 'BAG-waarden met verkeerde informatie', 'Onjuiste BAG-waarden'],
        title='Soort informatie')
DifNULL.on_change('value', update_DifNULL)

# plaats-select object initialiseren en interactief maken
# gemeenten, woonplaatsSelect = getAllWoonplaats()
gemeenten = pd.read_csv(str(direc) + '/work/Marnix/woz-board/Data/woonplaatsen.csv', dtype={'GEMEENTECODE' : str})
woonplaatsSelect = pd.read_csv(str(direc) + '/work/Marnix/woz-board/Data/wooSelect.csv')
woonplaatsSelect = list(woonplaatsSelect['list'])
plaats = bokeh.models.widgets.inputs.Select(options=woonplaatsSelect,
        title='Gemeente', max_width=300)
plaats.on_change('value', update_plaats)

# Filter checkboxes initialiseren en interactief maken
capitalize = CheckboxGroup(labels=["Controleer op hoofdletters", "Controleer op interpunctie", "Controleer op afkortingen"], active=[0, 1, 2], align='center')
capitalize.on_click(update_capitalize)

# Knoppen initialiseren en interactief maken


liveleader = bokeh.models.widgets.CheckboxButtonGroup(labels=['Live data', 'Leaderboard'], active=[0], align='end')
liveleader.on_change('active', update_liveleader)

plaatst = bokeh.layouts.row(plaats, liveleader)
# Hoofdlayout van dashboard initialiseren
layout = bokeh.layouts.layout(children=[plaatst], sizing_mode='scale_width')

# Webpaginatitel aangeven
doc.title = 'Kadaster | WOZ-Board'

# Hoofdlayout in website neerzetten
doc.add_root(layout)