import streamlit as st
import requests
import shapefile
import tempfile
import os
import zipfile
import shutil
from io import BytesIO
import urllib3
import xml.etree.ElementTree as ET
import pandas as pd
import time
import gc
import re
import json

# --- Configuration & SSL Setup ---
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
st.set_page_config(page_title="VZD Cadastre Merger", page_icon="🌍", layout="wide")

# --- Constants ---
DATASET_ID = "kadastra-informacijas-sistemas-atverti-telpiskie-dati"
TEXT_DATASET_ID = "kadastra-informacijas-sistemas-atvertie-dati"
CKAN_API_URL = f"https://data.gov.lv/dati/lv/api/3/action/package_show?id={DATASET_ID}"
TEXT_API_URL = f"https://data.gov.lv/dati/lv/api/3/action/package_show?id={TEXT_DATASET_ID}"

# --- HARDCODED ATVK MAP ---
ATVK_MAP = {
    # State Cities
    "rīga": "0001000", "daugavpils": "0002000", "jelgava": "0003000", "jūrmala": "0004000",
    "liepāja": "0005000", "rēzekne": "0006000", "ventspils": "0007000",
    # Counties (Novadi)
    "aizkraukles novads": "0020000", "alūksnes novads": "0021000", "augšdaugavas novads": "0022000",
    "ādažu novads": "0023000", "balvu novads": "0024000", "bauskas novads": "0025000",
    "cēsu novads": "0026000", "dienvidkurzemes novads": "0027000", "dobeles novads": "0028000",
    "gulbenes novads": "0029000", "jelgavas novads": "0030000", "jēkabpils novads": "0031000",
    "krāslavas novads": "0032000", "kuldīgas novads": "0033000", "ķekavas novads": "0034000",
    "limbažu novads": "0035000", "līvānu novads": "0036000", "ludzas novads": "0037000",
    "madonas novads": "0038001", "mārupes novads": "0039000", "ogres novads": "0040000",
    "olaines novads": "0041000", "preiļu novads": "0042000", "rēzeknes novads": "0043000",
    "ropažu novads": "0044000", "salaspils novads": "0045000", "saldus novads": "0046000",
    "saulkrastu novads": "0047000", "siguldas novads": "0048000", "smiltenes novads": "0049000",
    "talsu novads": "0051000", "tukuma novads": "0052000", "valkas novads": "0053000",
    "valmieras novads": "0054000", "ventspils novads": "0056000"
}

# --- Custom Styling (CSS) ---
st.markdown("""
    <style>
    .stButton>button { 
        width: 100%; 
        border-radius: 5px; 
        height: 3em; 
        background-color: #007bff; 
        color: white; 
        font-weight: bold; 
    }
    /* Disabled button styling override to make it clear it's not clickable */
    .stButton>button:disabled {
        background-color: #cccccc !important;
        color: #666666 !important;
        cursor: not-allowed;
    }
    [data-testid="stDownloadButton"] button {
        width: 100%;
        height: 3.5em;
        background-color: #ff8c00 !important; 
        color: white !important;
        font-size: 1.1em;
        font-weight: bold;
        border: none;
        border-radius: 8px;
        transition: background-color 0.3s ease;
    }
    [data-testid="stDownloadButton"] button:hover {
        background-color: #e67e22 !important; 
        color: white !important;
    }
    div[data-baseweb="select"] span[data-baseweb="tag"] { max-width: 100% !important; white-space: normal !important; height: auto !important; }
    div[data-baseweb="select"] span[data-baseweb="tag"] span { white-space: normal !important; word-break: break-word !important; }
    
    /* Updated Counter CSS */
    .counter-container { 
        display: flex; 
        justify-content: flex-end; 
        width: 100%; 
        margin-top: 30px; 
        margin-bottom: 10px;
    }
    .counter-box { 
        background-color: #e2e8f0; /* Greyish */
        color: #475569; 
        padding: 10px 20px; 
        border-radius: 8px; 
        font-size: 1em; 
        font-weight: bold; 
        border: 1px solid #cbd5e1; 
        box-shadow: 0 2px 4px rgba(0,0,0,0.05);
    }
    </style>
    """, unsafe_allow_html=True)

# --- Metadata Definition ---
FIELD_CONFIG = {
    'property': {
        'name': '1. Īpašumi (Properties)',
        'fields': { 'PRO_CAD_NR': 'Nekustamā īpašuma kadastra numurs', 'PRO_NAME': 'Nekustamā īpašuma nosaukums' }
    },
    'ownership': {
        'name': '2. Īpašumtiesības (Ownership)',
        'fields': { 'OWNERSHIP': 'Īpašuma tiesību statuss', 'PERSON': 'Personas statuss' }
    },
    'land': {
        'name': '3. Zemes vienības (Land)',
        'fields': { 'ATVK': 'ATVK kods', 'PAR_AREA': 'Platība (m2)', 'PURL_MAX': 'Lielākais NĪLM kods', 'P_AREA_MAX': 'Lielākā NĪLM platība', 'PURL_LST': 'NĪLM saraksts', 'P_AREA_LST': 'NĪLM platību saraksts', 'LIZ_QUAL': 'LIZ vērtējums (balles)' }
    },
    'building': {
        'name': '5. Būves (Buildings)',
        'fields': { 'BUI_NAME': 'Būves nosaukums', 'GLV': 'Galvenais lietošanas veids (kods)', 'GLV_NAME': 'Galvenais lietošanas veids (nosaukums)', 'BUI_AREA': 'Būves kopplatība', 'FLOORS': 'Virszemes stāvu skaits', 'U_FLOORS': 'Pazemes stāvu skaits', 'PG_COUNT': 'Telpu grupu skaits', 'EUG': 'Ekspluatācijas uzsākšanas gads', 'NOL': 'Nolietojums', 'NOT_EXIST': 'Pazīme par būves neesamību' }
    },
    'address': {
        'name': '7. Adreses (Addresses)',
        'fields': { 'ADDRESS': 'Adrese' }
    }
}

# --- Helper Functions ---
def get_target_atvks(sel_names):
    target_codes = set()
    debug_info = []
    for name in sel_names:
        clean_name = re.sub(r'^\d+\.\s*', '', name).lower().strip()
        code = ATVK_MAP.get(clean_name)
        if not code:
            short_name = clean_name.replace(" novads", "").replace(" pilsēta", "").replace(" valstspilsēta", "").strip()
            for map_name, map_code in ATVK_MAP.items():
                if short_name in map_name:
                    code = map_code
                    break
        if code:
            target_codes.add(code)
            debug_info.append(f"✅ '{name}' -> Cleaned: '{clean_name}' -> Code: **{code}**")
        else:
            debug_info.append(f"❌ '{name}' -> Cleaned: '{clean_name}' -> Not in Code Map")
    return target_codes, debug_info

# --- GitHub Gist Counter Setup ---
try:
    GITHUB_TOKEN = st.secrets["GITHUB_TOKEN"]
    GIST_ID = st.secrets["GIST_ID"]
    GIST_URL = f"https://api.github.com/gists/{GIST_ID}"
    GIST_HEADERS = {
        "Authorization": f"token {GITHUB_TOKEN}",
        "Accept": "application/vnd.github.v3+json",
    }
except Exception as e:
    GIST_URL = None
    GIST_HEADERS = None

def get_counter():
    if not GIST_URL: return 0
    try:
        response = requests.get(GIST_URL, headers=GIST_HEADERS, timeout=5)
        if response.status_code == 200:
            gist_data = response.json()
            content = gist_data["files"]["counter.json"]["content"]
            return json.loads(content).get("count", 0)
    except: pass
    return 0

def update_counter():
    if not GIST_URL: return 1
    current_count = get_counter()
    new_count = current_count + 1
    
    payload = {
        "files": {
            "counter.json": {
                "content": json.dumps({"count": new_count})
            }
        }
    }
    try:
        requests.patch(GIST_URL, headers=GIST_HEADERS, json=payload, timeout=5)
    except: pass
    return new_count

@st.cache_data
def get_territory_list():
    headers = {'User-Agent': 'Mozilla/5.0'}
    try:
        response = requests.get(CKAN_API_URL, headers=headers, verify=False, timeout=10)
        data = response.json()
        resources = {}
        if data.get('success'):
            for res in data['result']['resources']:
                fmt = res.get('format', '').upper()
                url = res.get('url', '')
                name = res.get('name', '')
                if (fmt in ['SHP', 'ZIP'] or url.lower().endswith('.zip')) and name and url:
                    resources[name] = url
        return resources
    except: return {}

@st.cache_data
def get_text_resources():
    headers = {'User-Agent': 'Mozilla/5.0'}
    urls = {'land': None, 'address': None, 'property': None, 'ownership': None, 'building': None}
    try:
        r = requests.get(TEXT_API_URL, headers=headers, verify=False, timeout=10)
        data = r.json()
        if data.get('success'):
            for res in data['result']['resources']:
                name = res.get('name', '')
                if "1." in name and "Nekustamo īpašumu" in name: urls['property'] = res['url']
                if "2." in name and "pašumtiesību" in name: urls['ownership'] = res['url']
                if "3." in name and "Zemes vienību" in name: urls['land'] = res['url']
                if "5." in name and "Būv" in name: urls['building'] = res['url']
                if "7." in name and "adreses" in name: urls['address'] = res['url']
    except: pass
    return urls

def normalize_id(text):
    if text is None: return ""
    s = str(text).replace(" ", "").strip()
    return s.zfill(11) if s.isdigit() and len(s) < 11 else s

def find_child(elem, suffix):
    if elem is None: return None
    for child in elem:
        if child.tag.endswith(suffix):
            return child
    return None

def find_text(elem, suffix, default=""):
    child = find_child(elem, suffix)
    return child.text.strip() if child is not None and child.text else default

# --- XML Parsing Functions ---
def format_lv_address(elem):
    parts = []
    street = (elem.findtext("Street") or "").strip()
    house = (elem.findtext("House") or "").strip()
    if street and house: parts.append(f"{street} {house}")
    elif street: parts.append(street)
    elif house: parts.append(house)
    village = (elem.findtext("Village") or "").strip()
    if village: parts.append(village)
    loc = []
    parish = (elem.findtext("Parish") or "").strip()
    town = (elem.findtext("Town") or "").strip()
    if parish: loc.append(parish)
    if town: loc.append(town)
    if loc: parts.append(", ".join(loc))
    county = (elem.findtext("County") or "").strip()
    if county: parts.append(county)
    p_idx = (elem.findtext("PostIndex") or "").strip()
    if p_idx:
        if p_idx.startswith("LV") and "-" not in p_idx: p_idx = p_idx.replace("LV", "LV-")
        parts.append(p_idx)
    return ", ".join(parts)

def parse_address_xml(xml_path):
    addr_map = {}
    try:
        context = ET.iterparse(xml_path, events=("end",))
        for event, elem in context:
            if elem.tag.endswith("AddressItemData"):
                rel = elem.find("ObjectRelation")
                if rel is not None:
                    cad_nr = rel.findtext("ObjectCadastreNr")
                    addr_data = elem.find("AddressData")
                    if cad_nr and addr_data is not None:
                        addr_map[normalize_id(cad_nr)] = format_lv_address(addr_data)
                elem.clear()
    except: pass
    return addr_map

def parse_land_xml(xml_path):
    data_map = {}
    try:
        context = ET.iterparse(xml_path, events=("end",))
        for event, elem in context:
            if elem.tag.endswith("ParcelItemData"):
                basic = elem.find("ParcelBasicData")
                if basic is not None:
                    cad_nr = basic.findtext("ParcelCadastreNr")
                    if cad_nr:
                        cid = normalize_id(cad_nr)
                        purposes = []
                        p_list = elem.find("LandPurposeList")
                        if p_list is not None:
                            for p_data in p_list.findall("LandPurposeData"):
                                kind = p_data.find("LandPurposeKind")
                                code = (kind.findtext("LandPurposeKindId") or "?").strip() if kind is not None else "?"
                                area = float(p_data.findtext("LandPurposeArea") or 0.0)
                                purposes.append((code, area))
                        purposes.sort(key=lambda x: (-x[1], x[0]))
                        data_map[cid] = {
                            'ATVK': (basic.findtext("ATVKCode") or "").strip(),
                            'PAR_AREA': float(basic.findtext("ParcelArea") or 0.0),
                            'PURL_MAX': purposes[0][0] if purposes else "",
                            'P_AREA_MAX': purposes[0][1] if purposes else 0.0,
                            'PURL_LST': ";".join([p[0] for p in purposes]),
                            'P_AREA_LST': ";".join([str(int(p[1])) for p in purposes]),
                            'LIZ_QUAL': float(basic.findtext("ParcelLizValue") or 0.0)
                        }
                elem.clear()
    except: pass
    return data_map

def parse_building_xml(xml_path):
    data_map = {}
    try:
        context = ET.iterparse(xml_path, events=("end",))
        for event, elem in context:
            if elem.tag.endswith("BuildingItemData"):
                basic = find_child(elem, "BuildingBasicData")
                if basic is not None:
                    cad_nr = find_text(basic, "BuildingCadastreNr")
                    if cad_nr:
                        cid = normalize_id(cad_nr)
                        def parse_int(val):
                            try: return int(val)
                            except: return 0
                        def parse_float(val):
                            try: return float(val)
                            except: return 0.0
                        use_kind = find_child(basic, "BuildingUseKind")
                        glv = find_text(use_kind, "BuildingUseKindId") if use_kind is not None else ""
                        glv_name = find_text(use_kind, "BuildingUseKindName") if use_kind is not None else ""
                        data_map[cid] = {
                            'Prereg': find_text(basic, "Prereg"), 
                            'BUI_NAME': find_text(basic, "BuildingName"),
                            'GLV': glv,
                            'GLV_NAME': glv_name,
                            'BUI_AREA': parse_float(find_text(basic, "BuildingArea")),
                            'FLOORS': parse_int(find_text(basic, "BuildingGroundFloors")),
                            'U_FLOORS': parse_int(find_text(basic, "BuildingUndergroundFloors")),
                            'PG_COUNT': parse_int(find_text(basic, "BuildingPregCount")),
                            'EUG': find_text(basic, "BuildingExploitYear"),
                            'NOL': find_text(basic, "BuildingDeprecation"),
                            'NOT_EXIST': find_text(basic, "NotExist")
                        }
                elem.clear()
    except Exception as e: pass
    return data_map

def parse_property_xml(xml_path):
    prop_map_sets = {}
    name_map_sets = {}
    try:
        context = ET.iterparse(xml_path, events=("end",))
        for event, elem in context:
            if elem.tag.endswith("PropertyItemData"):
                cad_obj_data = elem.find("CadastreObjectIdData")
                pro_cad_nr = (cad_obj_data.findtext("ProCadastreNr") or "").strip() if cad_obj_data is not None else None
                basic_data = elem.find("PropertyBasicData")
                pro_name = (basic_data.findtext("PropertyName") or "").strip() if basic_data is not None else None

                if pro_cad_nr:
                    content_data = elem.find("PropertyContentData")
                    if content_data is not None:
                        obj_list = content_data.find("ObjectList")
                        if obj_list is not None:
                            for obj_data in obj_list.findall("ObjectData"):
                                obj_cad_nr = obj_data.findtext("ObjectCadastreNrData")
                                if obj_cad_nr:
                                    pid = normalize_id(obj_cad_nr)
                                    if pid not in prop_map_sets: prop_map_sets[pid] = set()
                                    prop_map_sets[pid].add(pro_cad_nr)
                                    if pro_name:
                                        if pid not in name_map_sets: name_map_sets[pid] = set()
                                        name_map_sets[pid].add(pro_name)
                elem.clear()
    except: pass
    return (
        {k: ";".join(sorted(v)) for k, v in prop_map_sets.items()},
        {k: "; ".join(sorted(v)) for k, v in name_map_sets.items()}
    )

def parse_ownership_xml(xml_path):
    own_map = {}
    try:
        context = ET.iterparse(xml_path, events=("end",))
        for event, elem in context:
            if elem.tag.endswith("OwnershipItemData"):
                rel = elem.find("ObjectRelation")
                if rel is not None:
                    target_id = (rel.findtext("ObjectCadastreNr") or "").strip()
                    if target_id:
                        tid = normalize_id(target_id)
                        if tid not in own_map: own_map[tid] = {'statuses': set(), 'persons': set()}
                        status_list = elem.find("OwnershipStatusKindList")
                        if status_list is not None:
                            for kind in status_list.findall("OwnershipStatusKind"):
                                o_stat = (kind.findtext("OwnershipStatus") or "").strip()
                                p_stat = (kind.findtext("PersonStatus") or "").strip()
                                if o_stat: own_map[tid]['statuses'].add(o_stat)
                                if p_stat: own_map[tid]['persons'].add(p_stat)
                elem.clear()
    except: pass
    return own_map

def get_ownership_info(obj_id, prop_ids_str, ownership_map):
    collected_status = set()
    collected_person = set()
    ids_to_check = set()
    if obj_id: ids_to_check.add(obj_id)
    if prop_ids_str: ids_to_check.update(prop_ids_str.split(";"))
    for i in ids_to_check:
        clean_id = normalize_id(i)
        if clean_id in ownership_map:
            collected_status.update(ownership_map[clean_id]['statuses'])
            collected_person.update(ownership_map[clean_id]['persons'])
    return "; ".join(sorted(collected_status)), "; ".join(sorted(collected_person))

# --- Merging Shapefiles Logic ---
def merge_files(file_paths, out_path, land_map, build_map, addr_map, prop_map, name_map, own_map, selected_fields, is_parcel=True, prereg_mode=False):
    if not file_paths: return 0
    reference_fields = None
    kad_idx = -1
    for path in file_paths:
        try:
            with shapefile.Reader(path, encoding="utf-8") as sf:
                if len(sf.fields) > 1:
                    reference_fields = list(sf.fields)
                    f_names = [f[0].upper() for f in reference_fields]
                    for i, name in enumerate(f_names):
                        if name in ["CODE", "KAD_APZ", "PARCELCADASTRENR", "BUILDINGCADASTRENR"]: 
                            kad_idx = i - 1
                            break
                    break
        except: continue
    if not reference_fields: return 0

    field_defs = {
        'ADDRESS': ('C', 254), 'PRO_CAD_NR': ('C', 254), 'PRO_NAME': ('C', 254),
        'OWNERSHIP': ('C', 254), 'PERSON': ('C', 254), 
        'ATVK': ('C', 10), 'PAR_AREA': ('N', 12, 2), 'PURL_MAX': ('C', 10), 'P_AREA_MAX': ('N', 12, 2),
        'PURL_LST': ('C', 254), 'P_AREA_LST': ('C', 254), 'LIZ_QUAL': ('N', 12, 2),
        'BUI_NAME': ('C', 254), 'GLV': ('C', 10), 'GLV_NAME': ('C', 254),
        'BUI_AREA': ('N', 12, 2), 'FLOORS': ('N', 10, 0), 'U_FLOORS': ('N', 10, 0),
        'PG_COUNT': ('N', 10, 0), 'EUG': ('C', 50), 'NOL': ('C', 50), 'NOT_EXIST': ('C', 254)
    }

    active_field_names = []
    preferred_order = [
        'ADDRESS', 'PRO_CAD_NR', 'PRO_NAME', 'OWNERSHIP', 'PERSON', 
        'ATVK', 'PAR_AREA', 'PURL_MAX', 'P_AREA_MAX', 'PURL_LST', 'P_AREA_LST', 'LIZ_QUAL',
        'BUI_NAME', 'GLV', 'GLV_NAME', 'BUI_AREA', 'FLOORS', 'U_FLOORS', 'PG_COUNT', 'EUG', 'NOL', 'NOT_EXIST'
    ]
    
    count = 0
    with shapefile.Writer(out_path, encoding="utf-8") as w:
        for field in reference_fields:
            if field[0] != 'DeletionFlag': w.field(*field)

        for f_name in preferred_order:
            if f_name in selected_fields:
                if not is_parcel and f_name in ['ATVK', 'PAR_AREA', 'PURL_MAX', 'P_AREA_MAX', 'PURL_LST', 'P_AREA_LST', 'LIZ_QUAL']:
                    continue
                if is_parcel and f_name in ['BUI_NAME', 'GLV', 'GLV_NAME', 'BUI_AREA', 'FLOORS', 'U_FLOORS', 'PG_COUNT', 'EUG', 'NOL', 'NOT_EXIST']:
                    continue
                w.field(f_name, *field_defs[f_name])
                active_field_names.append(f_name)

        for path in file_paths:
            try:
                with shapefile.Reader(path, encoding="utf-8") as sf:
                    if len(sf.fields) != len(reference_fields): continue
                    for sr in sf.iterShapeRecords():
                        cid = normalize_id(sr.record[kad_idx]) if kad_idx != -1 else ""
                        row_data = list(sr.record)
                        
                        b_data = build_map.get(cid, {}) if not is_parcel else {}
                        
                        if not is_parcel:
                            prereg_val = str(b_data.get('Prereg', '')).strip()
                            is_prereg = (prereg_val == 'Pirmsreģistrēta būve')
                            
                            if prereg_mode and not is_prereg:
                                continue
                            if not prereg_mode and is_prereg:
                                continue
                        
                        prop_cads = prop_map.get(cid, "")
                        if len(prop_cads) > 254: prop_cads = prop_cads[:254]
                        
                        prop_names = name_map.get(cid, "")
                        if len(prop_names) > 254: prop_names = prop_names[:254]
                        
                        own_status, own_person = "", ""
                        if 'OWNERSHIP' in selected_fields or 'PERSON' in selected_fields:
                             own_status, own_person = get_ownership_info(cid, prop_cads, own_map)
                        
                        l_data = land_map.get(cid, {}) if is_parcel else {}
                        
                        for f_name in active_field_names:
                            val = None
                            if f_name == 'ADDRESS': val = addr_map.get(cid, "")
                            elif f_name == 'PRO_CAD_NR': val = prop_cads
                            elif f_name == 'PRO_NAME': val = prop_names
                            elif f_name == 'OWNERSHIP': val = own_status
                            elif f_name == 'PERSON': val = own_person
                            elif f_name == 'ATVK': val = l_data.get('ATVK', "")
                            elif f_name == 'PAR_AREA': val = l_data.get('PAR_AREA')
                            elif f_name == 'PURL_MAX': val = l_data.get('PURL_MAX', "")
                            elif f_name == 'P_AREA_MAX': val = l_data.get('P_AREA_MAX', 0.0)
                            elif f_name == 'PURL_LST': val = l_data.get('PURL_LST', "")
                            elif f_name == 'P_AREA_LST': val = l_data.get('P_AREA_LST', "")
                            elif f_name == 'LIZ_QUAL': val = l_data.get('LIZ_QUAL')
                            elif f_name == 'BUI_NAME': val = b_data.get('BUI_NAME', "")
                            elif f_name == 'GLV': val = b_data.get('GLV', "")
                            elif f_name == 'GLV_NAME': val = b_data.get('GLV_NAME', "")
                            elif f_name == 'BUI_AREA': val = b_data.get('BUI_AREA', 0.0)
                            elif f_name == 'FLOORS': val = b_data.get('FLOORS', 0)
                            elif f_name == 'U_FLOORS': val = b_data.get('U_FLOORS', 0)
                            elif f_name == 'PG_COUNT': val = b_data.get('PG_COUNT', 0)
                            elif f_name == 'EUG': val = b_data.get('EUG', "")
                            elif f_name == 'NOL': val = b_data.get('NOL', "")
                            elif f_name == 'NOT_EXIST': val = b_data.get('NOT_EXIST', "")
                            
                            if isinstance(val, str) and len(val) > 254: val = val[:254]
                            row_data.append(val)
                        
                        w.record(*row_data)
                        w.shape(sr.shape)
                        count += 1
            except: continue
            
    p = file_paths[0].replace(".shp", ".prj")
    if os.path.exists(p): shutil.copy(p, out_path.replace(".shp", ".prj"))
    with open(out_path.replace(".shp", ".cpg"), "w") as cpg: cpg.write("UTF-8")
    
    return count

def process_territories(sel_names, res_map, sel_types, txt_urls, join_text, selected_fields, prereg_mode=False):
    global_start_time = time.time()
    status = st.empty()
    progress_bar = st.progress(0)
    target_atvk_codes, debug_logs = get_target_atvks(sel_names)
    headers = {'User-Agent': 'Mozilla/5.0'}
    counts = {"Parcels": 0, "Buildings": 0, "Prereg_Buildings": 0}
    needed_datasets = set()
    if join_text:
        for ds_key, config in FIELD_CONFIG.items():
            if any(f in selected_fields for f in config['fields']):
                needed_datasets.add(ds_key)
        if 'ownership' in needed_datasets: needed_datasets.add('property')
        if "KKParcel" not in sel_types: needed_datasets.discard('land')
        if "KKBuilding" not in sel_types: needed_datasets.discard('building')

    tmp_dir = tempfile.mkdtemp()
    try:
        p_m, b_m = [], []
        for i, t in enumerate(sel_names):
            total_elapsed = round(time.time() - global_start_time, 1)
            status.text(f"Downloading Map: {t} ({i+1}/{len(sel_names)}) | Total Elapsed: {total_elapsed} s")
            progress_bar.progress((i + 1) / (len(sel_names) + 1)) 
            r = requests.get(res_map[t], headers=headers, verify=False, timeout=60)
            with zipfile.ZipFile(BytesIO(r.content)) as z:
                for f in z.namelist():
                    if any(x in f for x in ["KKParcel", "KKBuilding"]) and "Part" not in f and f.lower().endswith(".shp"):
                        for rf in [x for x in z.namelist() if x.startswith(f.rsplit('.', 1)[0])]: z.extract(rf, tmp_dir)
                        if "KKParcel" in f: p_m.append(os.path.join(tmp_dir, f))
                        else: b_m.append(os.path.join(tmp_dir, f))
        
        l_map, a_map, p_map, n_map, o_map, b_map = {}, {}, {}, {}, {}, {}
        
        if needed_datasets:
            tasks = [ ('address', 'Address', parse_address_xml), ('land', 'Land', parse_land_xml), ('building', 'Building', parse_building_xml), ('property', 'Property', parse_property_xml), ('ownership', 'Ownership', parse_ownership_xml) ]
            for key, msg, parse_func in tasks:
                if key in needed_datasets and txt_urls.get(key):
                    status.text(f"Downloading {msg}...")
                    r = requests.get(txt_urls[key], headers=headers, verify=False, stream=True)
                    with zipfile.ZipFile(BytesIO(r.content)) as z:
                        all_files = z.namelist()
                        xmls = []
                        if target_atvk_codes:
                            for f in all_files:
                                if f.lower().endswith('.xml') and any(part.startswith(c) for part in f.split('/') for c in target_atvk_codes):
                                    xmls.append(f)
                        if not xmls: xmls = [f for f in all_files if f.lower().endswith('.xml')]
                        
                        total_files = len(xmls)
                        for i, x in enumerate(xmls):
                            z.extract(x, tmp_dir)
                            parsed_data = parse_func(os.path.join(tmp_dir, x))
                            if key == 'ownership':
                                for k, v in parsed_data.items():
                                    if k not in o_map: o_map[k] = {'statuses': set(), 'persons': set()}
                                    o_map[k]['statuses'].update(v['statuses'])
                                    o_map[k]['persons'].update(v['persons'])
                            elif key == 'property':
                                p_ids, p_names = parsed_data
                                for k, v in p_ids.items():
                                    p_map[k] = ";".join(sorted(set(p_map[k].split(";")) | set(v.split(";")))) if k in p_map else v
                                for k, v in p_names.items():
                                    n_map[k] = "; ".join(sorted(set(n_map[k].split("; ")) | set(v.split("; ")))) if k in n_map else v
                            elif key == 'address': a_map.update(parsed_data)
                            elif key == 'land': l_map.update(parsed_data)
                            elif key == 'building': b_map.update(parsed_data)
                                    
                            os.remove(os.path.join(tmp_dir, x))
                            if i % 10 == 0 or i == total_files - 1:
                                progress_bar.progress((i + 1) / total_files if total_files > 0 else 1.0)
                                status.text(f"Processing {msg} | File {i+1}/{total_files} | Time: {round(time.time() - global_start_time, 1)} s")

        progress_bar.empty()
        zip_buf = BytesIO()
        with zipfile.ZipFile(zip_buf, "w", zipfile.ZIP_DEFLATED) as zf:
            success = False
            status.text("Merging Shapefiles...")
            
            if "KKParcel" in sel_types and p_m and not prereg_mode:
                out = os.path.join(tmp_dir, "Merged_Parcels.shp")
                c = merge_files(p_m, out, l_map, {}, a_map, p_map, n_map, o_map, selected_fields, True, prereg_mode=False)
                if c > 0:
                    for e in [".shp", ".shx", ".dbf", ".prj", ".cpg"]: zf.write(out.replace(".shp", e), f"Merged_Parcels{e}")
                    counts["Parcels"] = c
                    success = True
            
            if "KKBuilding" in sel_types and b_m:
                out_name = "Prereg_Buildings" if prereg_mode else "Merged_Buildings"
                out = os.path.join(tmp_dir, f"{out_name}.shp")
                c = merge_files(b_m, out, {}, b_map, a_map, p_map, n_map, o_map, selected_fields, False, prereg_mode=prereg_mode)
                if c > 0:
                    for e in [".shp", ".shx", ".dbf", ".prj", ".cpg"]: zf.write(out.replace(".shp", e), f"{out_name}{e}")
                    if prereg_mode:
                        counts["Prereg_Buildings"] = c
                    else:
                        counts["Buildings"] = c
                    success = True
            
        if success:
            status.text("Finished!")
            zip_buf.seek(0)
            return zip_buf.getvalue(), counts
    except Exception as e:
        status.error(f"Error: {e}")
        return None, counts
    finally:
        gc.collect() 
        for _ in range(5):
            try: 
                shutil.rmtree(tmp_dir)
                break
            except: 
                time.sleep(0.2)
    return None, counts

def process_excel_export(sel_names, txt_urls):
    global_start_time = time.time()
    status = st.empty()
    progress_bar = st.progress(0)
    
    target_atvk_codes, debug_logs = get_target_atvks(sel_names)
    headers = {'User-Agent': 'Mozilla/5.0'}
    tmp_dir = tempfile.mkdtemp()
    
    l_map, a_map, p_map, n_map, o_map, b_map = {}, {}, {}, {}, {}, {}
    
    tasks = [
        ('address', 'Address', parse_address_xml),
        ('land', 'Land', parse_land_xml),
        ('building', 'Building', parse_building_xml),
        ('property', 'Property', parse_property_xml),
        ('ownership', 'Ownership', parse_ownership_xml)
    ]

    try:
        for key, msg, parse_func in tasks:
            if txt_urls.get(key):
                status.text(f"Downloading {msg} XMLs...")
                r = requests.get(txt_urls[key], headers=headers, verify=False, stream=True)
                with zipfile.ZipFile(BytesIO(r.content)) as z:
                    all_files = z.namelist()
                    xmls = []
                    if target_atvk_codes:
                        for f in all_files:
                            if f.lower().endswith('.xml') and any(part.startswith(c) for part in f.split('/') for c in target_atvk_codes):
                                xmls.append(f)
                    if not xmls: xmls = [f for f in all_files if f.lower().endswith('.xml')]
                    
                    total_files = len(xmls)
                    for i, x in enumerate(xmls):
                        z.extract(x, tmp_dir)
                        parsed_data = parse_func(os.path.join(tmp_dir, x))
                        
                        if key == 'ownership':
                            for k, v in parsed_data.items():
                                if k not in o_map: o_map[k] = {'statuses': set(), 'persons': set()}
                                o_map[k]['statuses'].update(v['statuses'])
                                o_map[k]['persons'].update(v['persons'])
                        elif key == 'property':
                            p_ids, p_names = parsed_data
                            for k, v in p_ids.items():
                                p_map[k] = ";".join(sorted(set(p_map[k].split(";")) | set(v.split(";")))) if k in p_map else v
                            for k, v in p_names.items():
                                n_map[k] = "; ".join(sorted(set(n_map[k].split("; ")) | set(v.split("; ")))) if k in n_map else v
                        elif key == 'address': a_map.update(parsed_data)
                        elif key == 'land': l_map.update(parsed_data)
                        elif key == 'building': b_map.update(parsed_data)
                                
                        os.remove(os.path.join(tmp_dir, x))
                        if i % 10 == 0 or i == total_files - 1:
                            progress_bar.progress((i + 1) / total_files if total_files > 0 else 1.0)
                            status.text(f"Processing {msg} | File {i+1}/{total_files} | Time: {round(time.time() - global_start_time, 1)} s")

        status.text("Building Excel File...")
        progress_bar.empty()

        parcel_rows = []
        for cid in l_map.keys():
            p_cads = p_map.get(cid, "")
            o_stat, o_pers = get_ownership_info(cid, p_cads, o_map)
            parcel_rows.append({
                'CODE': cid, 'ADDRESS': a_map.get(cid, ""), 'PRO_CAD_NR': p_cads, 
                'PRO_NAME': n_map.get(cid, ""), 'OWNER_SHIP': o_stat, 'PERSON': o_pers
            })

        building_rows = []
        for cid, b_data in b_map.items():
            if str(b_data.get('Prereg', '')).strip() == 'Pirmsreģistrēta būve': continue
            p_cads = p_map.get(cid, "")
            o_stat, o_pers = get_ownership_info(cid, p_cads, o_map)
            building_rows.append({
                'CODE': cid, 'ADDRESS': a_map.get(cid, ""), 'PRO_CAD_NR': p_cads, 
                'PRO_NAME': n_map.get(cid, ""), 'OWNER_SHIP': o_stat, 'PERSON': o_pers
            })

        df_parcels = pd.DataFrame(parcel_rows)
        df_buildings = pd.DataFrame(building_rows)

        cols = ['CODE', 'ADDRESS', 'PRO_CAD_NR', 'PRO_NAME', 'OWNER_SHIP', 'PERSON']
        df_parcels = df_parcels.reindex(columns=cols) if not df_parcels.empty else pd.DataFrame(columns=cols)
        df_buildings = df_buildings.reindex(columns=cols) if not df_buildings.empty else pd.DataFrame(columns=cols)

        excel_buf = BytesIO()
        with pd.ExcelWriter(excel_buf, engine='openpyxl') as writer:
            df_parcels.to_excel(writer, sheet_name='Parcels', index=False)
            df_buildings.to_excel(writer, sheet_name='Buildings', index=False)
            
        excel_buf.seek(0)
        status.text(f"Excel generation finished! Total time: {round(time.time() - global_start_time, 1)} seconds.")
        return excel_buf.getvalue(), len(parcel_rows), len(building_rows)

    except Exception as e:
        status.error(f"Error during Excel generation: {e}")
        return None, 0, 0
    finally:
        gc.collect() 
        for _ in range(5):
            try: 
                shutil.rmtree(tmp_dir)
                break
            except: 
                time.sleep(0.2)


# --- Main App Interface ---
st.title("🌍 VZD Cadastre Merger")
st.markdown("Combines Spatial Polygons with selected textual data attributes (VZD XML), or exports textual metadata to Excel.")

with st.sidebar:
    st.header("⚙️ Configuration")
    st.markdown("Select your territories in the main window to begin processing.")
    st.divider()
    with st.spinner("Checking VZD Open Data..."):
        res_map = get_territory_list()
        txt_urls = get_text_resources()
    if res_map: st.success(f"Connected to {len(res_map)} territories")

if res_map:
    t_names = sorted(list(res_map.keys()), key=lambda x: int(x.split('.')[0]) if x.split('.')[0].isdigit() else 999)
    
    if "sel_territories" not in st.session_state:
        st.session_state["sel_territories"] = []
        
    sel_count = len(st.session_state["sel_territories"])
    label_text = f"📍 Select Territories ({sel_count} selected):" if sel_count > 0 else "📍 Select Territories:"
    
    sel = st.multiselect(label_text, t_names, key="sel_territories")
    
    if st.checkbox("Select All Territories"): 
        sel = t_names

    st.divider()

    # --- TABBED INTERFACE ---
    tab1, tab2, tab3 = st.tabs(["🚀 Merge Shapefiles", "📊 Export Property Owners (Excel)", "🏗️ Export Preregistered Buildings"])

    with tab1:
        st.markdown("Download standard `.shp` files with merged XML data. (Note: Text fields are capped at 255 characters due to database limits).")
        
        col1, col2 = st.columns([1, 1])
        with col1:
            types = st.multiselect("Select Layers to Export:", ["KKParcel", "KKBuilding"], default=["KKParcel", "KKBuilding"])
            
        with col2:
            join_text = st.toggle("Join XML Text Data", value=True)
            
        selected_fields = []
        if join_text:
            st.markdown("### 🗂️ Select Fields to Join")
            exp_cols = st.columns(3) 
            for i, (ds_key, config) in enumerate(FIELD_CONFIG.items()):
                with exp_cols[i % 3]:
                    with st.expander(config['name'], expanded=False):
                        field_options = list(config['fields'].keys())
                        selected = st.multiselect(
                            f"Fields",
                            options=field_options,
                            default=field_options,
                            format_func=lambda x: f"{x} ({config['fields'][x]})",
                            key=f"sel_{ds_key}"
                        )
                        selected_fields.extend(selected)
        
        btn1_disabled = len(sel) == 0 or len(types) == 0
        
        if st.button("🚀 Process and Generate Shapefiles", type="primary", disabled=btn1_disabled):
            start_time = time.time()
            final_data, counts = process_territories(sel, res_map, types, txt_urls, join_text, selected_fields, prereg_mode=False)
            elapsed_time = round(time.time() - start_time, 1)
            
            if final_data:
                # API UPDATE NOW HAPPENS HERE, HIDDEN BEHIND THE SCENES
                st.session_state["total_downloads"] = update_counter()
                
                st.success(f"Shapefile Data processed successfully in {elapsed_time} seconds!")
                
                summary_data = [{"Layer": l, "Total Polygons": c} for l, c in counts.items() if c > 0]
                if summary_data:
                    st.table(pd.DataFrame(summary_data))

                # PURE NATIVE DOWNLOAD - ZERO CALLBACKS
                st.download_button(
                    label="📥 Download Merged Shapefiles (.zip)",
                    data=final_data,
                    file_name="cadastre_merged.zip",
                    mime="application/zip"
                )

    with tab2:
        st.markdown("""
        Export (`CODE`, `ADDRESS`, `PRO_CAD_NR`, `PRO_NAME`, `OWNER_SHIP`, `PERSON`) directly into an `.xlsx` file. 
        **Advantage:** `PRO_CAD_NR` lists are not truncated and remain fully intact regardless of length.
        """)
        
        btn2_disabled = len(sel) == 0
        
        if st.button("📊 Generate Excel Export", type="primary", key="excel_btn", disabled=btn2_disabled):
            start_time = time.time()
            excel_data, p_count, b_count = process_excel_export(sel, txt_urls)
            elapsed_time = round(time.time() - start_time, 1)
            
            if excel_data:
                # API UPDATE NOW HAPPENS HERE, HIDDEN BEHIND THE SCENES
                st.session_state["total_downloads"] = update_counter()
                
                st.success(f"Excel File Built Successfully in {elapsed_time} seconds!")
                
                st.table(pd.DataFrame([
                    {"Sheet": "Parcels", "Total Records": p_count},
                    {"Sheet": "Buildings", "Total Records": b_count}
                ]))
                
                # PURE NATIVE DOWNLOAD - ZERO CALLBACKS
                st.download_button(
                    label="📥 Download Property_Owners.xlsx",
                    data=excel_data,
                    file_name="Property_Owners.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )

    with tab3:
        st.markdown("""
        Extract and export **ONLY** Preregistered Buildings ("Pirmsreģistrēta būve") as a Shapefile.
        This will automatically ignore Parcels and fully registered buildings.
        """)
        
        btn3_disabled = len(sel) == 0
        
        if st.button("🏗️ Generate Preregistered Buildings", type="primary", key="prereg_btn", disabled=btn3_disabled):
            start_time = time.time()
            final_data, counts = process_territories(sel, res_map, ["KKBuilding"], txt_urls, False, [], prereg_mode=True)
            elapsed_time = round(time.time() - start_time, 1)
            
            if final_data:
                # API UPDATE NOW HAPPENS HERE, HIDDEN BEHIND THE SCENES
                st.session_state["total_downloads"] = update_counter()
                
                st.success(f"Preregistered Buildings processed successfully in {elapsed_time} seconds!")
                
                summary_data = [{"Layer": l, "Total Polygons": c} for l, c in counts.items() if c > 0]
                if summary_data:
                    st.table(pd.DataFrame(summary_data))

                # PURE NATIVE DOWNLOAD - ZERO CALLBACKS
                st.download_button(
                    label="📥 Download Prereg_buildings.zip",
                    data=final_data,
                    file_name="Prereg_buildings.zip",
                    mime="application/zip"
                )

    with st.expander("ℹ️ Field Metadata"):
        st.markdown("""
        *Note: The output Shapefile will only contain fields selected in the configuration menu.*
        
        ### 🌍 General Fields
        | Field | Origin | Description |
        | :--- | :--- | :--- |
        | **CODE** | SHP | Kadastra apzīmējums |
        | **ADDRESS** | Dataset 7 | Adrese (standartpierakstā) |
        | **PRO_CAD_NR**| Dataset 1 | Nekustamā īpašuma kadastra numurs (var būt vairāki) |
        | **PRO_NAME** | Dataset 1 | Nekustamā īpašuma nosaukums |
        | **OWNERSHIP** | Dataset 2 | Īpašuma tiesību statuss (piem. Īpašnieks, Tiesiskais valdītājs) |
        | **PERSON** | Dataset 2 | Personas statuss (piem. Fiziska persona, Juridiska persona) |
        
        ### 🟩 Zemes Vienības (Parcels only)
        | Field | Origin | Description |
        | :--- | :--- | :--- |
        | **ATVK** | Dataset 3 | Administratīvi teritoriālās vienības kods |
        | **PAR_AREA** | Dataset 3 | Zemes vienības platība (teksta datos), m2 |
        | **PURL_MAX** | Dataset 3 | Pēc platības lielākais NĪLM |
        | **P_AREA_MAX** | Dataset 3 | Platība lielākajam NĪLM, m2 |
        | **PURL_LST** | Dataset 3 | NĪLM saraksts |
        | **P_AREA_LST** | Dataset 3 | NĪLM platību saraksts |
        | **LIZ_QUAL** | Dataset 3 | LIZ kvalitatīvais novērtējums ballēs |
        
        ### 🏢 Būves (Buildings only)
        | Field | Origin | Description |
        | :--- | :--- | :--- |
        | **BUI_NAME** | Dataset 5 | Būves nosaukums |
        | **GLV** | Dataset 5 | Galvenais lietošanas veids (kods) |
        | **GLV_NAME** | Dataset 5 | Galvenais lietošanas veids (nosaukums) |
        | **BUI_AREA** | Dataset 5 | Būves kopplatība |
        | **FLOORS** | Dataset 5 | Virszemes stāvu skaits |
        | **U_FLOORS** | Dataset 5 | Pazemes stāvu skaits |
        | **PG_COUNT** | Dataset 5 | Telpu grupu skaits |
        | **EUG** | Dataset 5 | Ekspluatācijas uzsākšanas gads |
        | **NOL** | Dataset 5 | Nolietojums |
        | **NOT_EXIST**| Dataset 5 | Pazīme par būves neesamību |
        """)

# --- Render Footer Counter at the absolute bottom ---
st.divider()

if "total_downloads" not in st.session_state:
    st.session_state["total_downloads"] = get_counter()

st.markdown(f'<div class="counter-container"><div class="counter-box">📥 Total Generated: {st.session_state["total_downloads"]}</div></div>', unsafe_allow_html=True)
