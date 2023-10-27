import gzip
import pandas as pd
import xml.etree.ElementTree as ET
from sys import argv
from typing import List, Dict, Any, Set
from functools import reduce, partial

def main():
    # load raw dict
    # looping through entries:
    ## I. collect:
    ### 1. (all) kanji (['k_ele'][i]['keb'])
    ### 2. (all) kana (['r_ele'][i]['reb'])
    ### 3. (all) meaning (['sense'][i]['gloss'])
    ### 4. (unique) part of speech (['sense'][i]['pos'])
    ### 4. (unique) reading info (['k_ele'][i]['ke_inf']) 
    ### 5. (unique) meaning info (['sense']['misc'])
    ### 6. flag uk: "word usually written using kana alone"
    ## II. search index:
    ### every entry in kanji and kana lists, each in row with id for table
    ## III. anki entry table:
    ### expression: all kanji entries unless uk, then all kana entries
    ### reading: opposite of previous
    ### pos: all part of speech entries
    ### meaning: each meaning '1. ...', with notes at end
    if not len(argv) == 3:
        print('Syntax: make_jmdict_tables.py [jmdict path] [output path]')
        exit()
    else:
        input_path = argv[1]
        output_path = argv[2]
    jmdict = load_dictionary(input_path)
    collected = collect_entries(jmdict)
    search_index = make_search_index(collected)
    import_table = make_import_table(collected)
    search_index.to_csv(output_path+'/search_index.csv.gz')
    import_table.to_csv(output_path+'/anki_entries.csv.gz', index=False)


def load_dictionary(path:str) -> List[Dict[str, Any]]:
    with gzip.open(path, mode='r') as f:
        tree = ET.parse(f)
    root = tree.getroot()
    jmdict = []
    for entry in root:
        new_entry = {}
        for element in entry:
            if element.tag == 'ent_seq':
                new_entry['id'] = int(element.text)
            else:
                if element.tag not in new_entry:
                    new_entry[element.tag] = []
                sub_dict = {}
                for sub_element in element:
                    tag = sub_element.tag
                    text = sub_element.text
                    if tag in sub_dict:
                        try:
                            sub_dict[tag] += ', '+text
                        except Exception as e:
                            pass
                    else:
                        sub_dict[tag] = text
                new_entry[element.tag].append(sub_dict)
        jmdict.append(new_entry)
    return jmdict


def collect_entries(jmdict:List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    collected:List[Dict[str, Any]] = []
    for entry in jmdict:
        id = entry['id']     
        kanji = []
        kanji_notes = set()
        kana = []
        meanings = []
        pos = set()
        notes = set()
        
        if 'k_ele' in entry:
            for k in entry['k_ele']:
                kanji.append(k.get('keb'))
                kanji_notes.add(k.get('ke_inf'))
        if 'r_ele' in entry:
            for reading in entry['r_ele']:
                kana.append(reading.get('reb'))
        if 'sense' in entry:
            for sense in entry['sense']:
                meanings.append(sense.get('gloss'))
                pos.add(sense.get('pos'))
                notes.add(sense.get('misc'))
        notes.discard(None)
        kanji_notes.discard(None)
        uk = ("word usually written using kana alone" in notes) or not kanji
        new_entry = make_entry(id, kanji, kanji_notes, kana, meanings, pos, notes, uk)
        collected.append(new_entry)
    return collected


def make_entry(id:int,
               kanji:List[str],
               kanji_notes:Set[str],
               kana:List[str],
               meanings:List[str],
               pos:Set[str],
               notes:Set[str],
               uk:bool
):
    return {
        'id': id,
        'kanji': kanji,
        'kanji_notes': list(kanji_notes),
        'kana': kana,
        'meanings': meanings,
        'pos': list(pos),
        'notes': list(notes),
        'use_kana': uk
    }
            

def make_search_index(entries:List[Dict[str, Any]]) -> pd.DataFrame:
    search_entries = []
    for entry in entries:
        id = entry['id']
        elements = entry['kanji'] + entry['kana']
        new_entries = [
            {'id':id, 'entry':element}
            for element in elements
        ]
        search_entries.extend(new_entries)
    return pd.DataFrame(search_entries)

def str_list(lst:List[str]) -> str:
    if len(lst) == 0:
        return ''
    elif len(lst) == 1:
        return lst[0]
    else:
        return reduce(lambda l,r: f'{l}, {r}', lst)

def make_import_table(entries:List[Dict[str, Any]]) -> pd.DataFrame:
    anki_entries = []
    # str_list = partial(reduce, lambda l,r: f'{l}, {r}')
    for entry in entries:
        if entry['use_kana']:
            expression = str_list(entry['kana'])
            reading = str_list(entry['kanji'])
        else:
            expression = str_list(entry['kanji'])
            reading = str_list(entry['kana'])
        if len(entry["kanji_notes"]) > 0:
            reading += f' ({str_list(entry["kanji_notes"])})'
        part_of_speech = str_list(entry['pos'])
        if len(entry['meanings']) == 1:
            meaning = str.capitalize(entry['meanings'][0] + '.')
        else:
            meaning = ''
            for i, element in enumerate(entry['meanings']):
                if i > 0:
                    meaning += '; '
                meaning += f'{i+1}. {element}'
            meaning += '.'
        if len(entry['notes']) > 0:
            meaning += f' ({str_list(entry["notes"])})'
        anki_entries.append({
            'id': entry['id'],
            'expression': expression,
            'meaning': meaning,
            'reading': reading,
            'part_of_speech': part_of_speech
        })
    return pd.DataFrame(anki_entries)


if __name__ == '__main__':
    main()
