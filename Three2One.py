# -*- coding: utf-8 -*-

# Process:
# 1. Given a list of geodatabase layers and specific fields which representing the oid or slopeid
# 2. Read the attribute tables from given geodatabase layer 
# 3. Merge the attribute tables into one table, remain the oid or slopeid field 
# 4. Open a new geodatabase
# 4. Iterate throught the merged table:
#    1.  For each row, find the corresponding feature in the geodatabase layers
#    2.  Merge the feature into a new feature class in the new geodatabase
# 5. Save the new geodatabase and export the new feature class into given format (e.g. shapefile)

from pathlib import Path
from typing import List
import argparse
from osgeo import ogr
import pandas as pd

def GetFeatureLayer(gdb_path):
    driver = ogr.GetDriverByName('FileGDB')
    gdb = driver.Open(gdb_path, 0)
    if gdb is None:
        print('Failed to open the geodatabase.')
        raise FileNotFoundError('Failed to open the geodatabase.')
        # return None
    else:
        print('Feature Layers in the Geodatabase:')
        feature_layers = []
        for i in range(gdb.GetLayerCount()):
            layer = gdb.GetLayerByIndex(i)
            feature_layers.append(layer)
            print(layer.GetName())
        return feature_layers

def GetFieldIndex(layer, field_name):
    layer_defn = layer.GetLayerDefn()
    for i in range(layer_defn.GetFieldCount()):
        if layer_defn.GetFieldDefn(i).GetName() == field_name:
            return i
    return -1

def GetFieldValues(layer, field_name):
    field_index = GetFieldIndex(layer, field_name)
    if field_index == -1:
        print('Field not found.')
        raise IndexError('Field not found.')
        # return None
    field_values = []
    for feature in layer:
        field_values.append(feature.GetField(field_index))
    return field_values

def GetGeometryWktList(layer):
    geometry_wkt_list = []
    for feature in layer:
        geometry = feature.GetGeometryRef()
        geometry_wkt_list.append(geometry.ExportToWkt())
    return geometry_wkt_list

def GetGeometryByIndex(layer, index):
    feature = layer.GetFeature(index)
    if feature is None:
        print('Failed to get the feature.')
        raise IndexError('Failed to get the feature.')
        return None
    geometry = feature.GetGeometryRef()
    return geometry

def ReadTableAsPandasDataframe(gdb_layer_path:Path, field_name:str, new_oid_name:str="OID"):
    """Read the attribute tables from given geodatabase layer, return the given field_name with new_field_name and the oid field as a pandas dataframe."""
    driver = ogr.GetDriverByName('ESRI Shapefile') #OpenFileGDB
    with driver.Open(str(gdb_layer_path.parent), 0) as gdb:
        if gdb is None:
            print('Failed to open the geodatabase.')
            return None
        else:
            layer = gdb.GetLayer(str(gdb_layer_path.stem))
            # print("Layer",layer.GetLayerDefn().GetFieldDefn(0).GetName())
            field_values = GetFieldValues(layer, field_name)
            #oid_values = GetFieldValues(layer, 'OBJECTID')
            if field_values is None : #or oid_values is None
                print('Failed to read the field values.')
                raise IndexError('Failed to read the field values.')
            df = pd.DataFrame({new_oid_name: range(len(field_values)),field_name: field_values}) # 'OBJECTID': oid_values
            return df

def Test_ReadTableAsPandasDataframe():
    gdb_layer_path = Path(r'./TestFCs/Line1.shp')
    field_name = 'SID1'
    new_field_name = 'SID11'
    df = ReadTableAsPandasDataframe(gdb_layer_path, field_name, new_field_name)
    print(df)
    # OK!

def MergeDataframesUsingSpecificFieldName(attribute_dataframes:List[pd.DataFrame], field_names:List[str]):
    """Merge the attribute tables into one table. Merge the dataframes using the field_names. When iterate the attribute_dataframe, rename the "OBJECTID" field to "OBJECTID_n" in the merged dataframe."""
    merged_df = attribute_dataframes[0]
    merged_df.rename(columns={'OBJECTID': f'OBJECTID_{str(0)}'}, inplace=True)
    for i in range(1, len(attribute_dataframes)):
        merged_df = pd.merge(merged_df, attribute_dataframes[i], left_on=field_names[i-1], right_on=field_names[i] ,how='inner')
        merged_df.rename(columns={'OBJECTID': f'OBJECTID_{str(i)}'}, inplace=True)
    return merged_df

def Test_MergeDataframesUsingSpecificFieldName():
    df1 = pd.DataFrame({'field1': [1, 2, 3], 'OBJECTID': [1, 2, 3]})
    df2 = pd.DataFrame({'field2': [3, 2, 1], 'OBJECTID': [1, 2, 3]})
    df3 = pd.DataFrame({'field3': [1, 3, 2], 'OBJECTID': [1, 2, 3]})
    attribute_dataframes = [df1, df2, df3]
    field_names = ['field1', 'field2', 'field3']
    merged_df = MergeDataframesUsingSpecificFieldName(attribute_dataframes, field_names)
    print(merged_df)
    # OK!

def Test_ThreeShpFileMergrTheAttributeTable():
    gdb_layer_path1 = Path(r'./TestFCs/Line1.shp')
    gdb_layer_path2 = Path(r'./TestFCs/Line2.shp')
    gdb_layer_path3 = Path(r'./TestFCs/Line3.shp')
    field_names = ['SID1', 'SID2', 'SID3']
    # df_list =  [ReadTableAsPandasDataframe(gdb_layer_path, field_name) for gdb_layer_path in gdb_layer_paths for field_name in field_names]
    df1 = ReadTableAsPandasDataframe(gdb_layer_path1, field_names[0], 'OID1')
    df2 = ReadTableAsPandasDataframe(gdb_layer_path2, field_names[1], 'OID2')
    df3 = ReadTableAsPandasDataframe(gdb_layer_path3, field_names[2], 'OID3')
    merged_df = MergeDataframesUsingSpecificFieldName([df1,df2,df3], field_names)
    print(merged_df)

def SaveFeaturesToInividualFineBasedOnMergedTable(merged_df:pd.DataFrame, layer_paths:List[Path],field_names:List[str],oid_names:List[str],ExportDirectory:Path):
    """Iterate throught the merged table and save the corresponding feature into a new feature class in the new geodatabase."""
    layer_counts = len(layer_paths)
    # Open all the existed feature classes
    input_datasets = [ogr.Open(str(layer_path),0) for layer_path in layer_paths]
    # print(f"Opened {layer_counts} datasets.")
    # Get the layer of input_datasets
    input_layers = [input_dataset.GetLayer(0) for input_dataset in input_datasets]
    # print(f"Opened {layer_counts} layers.")
    # Get the wkt lists of the input layers
    geometry_wkt_lists = [GetGeometryWktList(input_layer) for input_layer in input_layers]
    # print(geometry_wkt_lists)
    # Get the spatial reference of the first layer
    spatial_ref = input_layers[0].GetSpatialRef()
    geometry_type = input_layers[0].GetGeomType()

    # Iterate throught the merged table
    for index, row in merged_df.iterrows():
        # Create a new shapefile in the ExportDirectory
        # print(f"Processing {index} row...")
        new_layer_name = f'{field_names[0]}_{str(row[field_names[0]])}' # e.g. SID1_1
        # print(f"Creating {new_layer_name}...")
        new_dataset = ogr.GetDriverByName('ESRI Shapefile').CreateDataSource(str(ExportDirectory / f'{new_layer_name}.shp'))
        if new_dataset is None:
            # print('Failed to create the new feature.')
            raise FileNotFoundError('Failed to create the new feature.')
        # create new layer
        # print("create new layer...")
        new_layer = new_dataset.CreateLayer(new_layer_name, spatial_ref, geometry_type)
        # Iterate through the input layers and copy the feature to the new layer
        for i_input_layer in range(layer_counts): 
            # print(f"Copying feature from {layer_paths[i_input_layer]}...")
            # Write the geometry to the new feature
            featureDefn = new_layer.GetLayerDefn()
            new_feature = ogr.Feature(featureDefn)
            new_feature.SetGeometry(ogr.CreateGeometryFromWkt(geometry_wkt_lists[i_input_layer][row[oid_names[i_input_layer]]]))
            new_layer.CreateFeature(new_feature)
            new_feature = None
    
    # Close all datasets    
    for i_dataset in input_datasets:
        i_dataset = None
    new_dataset = None
    


def Three2One(layer_paths:List[Path], field_names:List[str], ExportDirectory:Path):
    """Create a new file and read all given feature classes in the geodatabase"""
    if not ExportDirectory.exists():
        ExportDirectory.mkdir(parents=True)
    # Read all input layers and create dataframes
    OID_names = [f'OID_{str(i)}' for i in range(len(layer_paths))]
    attribute_dataframes = [ReadTableAsPandasDataframe(layer_path, field_name,new_oid_name) for layer_path, field_name,new_oid_name in zip(layer_paths, field_names, OID_names)]
    # Merge the dataframes
    merged_df = MergeDataframesUsingSpecificFieldName(attribute_dataframes, field_names)
    # print(merged_df)
    # Save the features
    SaveFeaturesToInividualFineBasedOnMergedTable(merged_df, layer_paths, field_names, OID_names, ExportDirectory)

def Test_Three2One():
    layer_paths = [
        Path(r'./TestFCs/Line1.shp'), 
        Path(r'./TestFCs/Line2.shp'), 
        Path(r'./TestFCs/Line3.shp')]
    field_names = ['SID1', 'SID2', 'SID3']
    ExportDirectory = Path(r'./TestFCs/TestExport')
    Three2One(layer_paths, field_names, ExportDirectory)

def parse_arg():
    parser = argparse.ArgumentParser(description='Three2One')
    parser.add_argument('-i', type=Path, required=True, nargs='+', help='The paths of the feature classes.')
    parser.add_argument('-n', type=str, required=True, nargs='+', help='The field names.')
    parser.add_argument('-e', type=Path, required=True, help='The path of the export directory.')
    args = parser.parse_args()
    return args

if __name__ == '__main__':
    # Test_ReadTableAsPandasDataframe() #OK
    # Test_MergeDataframesUsingSpecificFieldName() #OK!
    # Test_ThreeShpFileMergrTheAttributeTable() #OK!
    # Test_Three2One() 
    args = parse_arg()
    if len(args.i) == len(args.n):
        Three2One(args.i, args.n, args.e)
    else:
        print('The number of input feature classes and field names should be the same.')
