
import sys
import pandas as pd
import logging

# Config fake logging
logging.basicConfig(level=logging.INFO)

# Import manager
from src.core.manager import GeoValidaManager

def diagnose():
    print("Initializing Manager...")
    manager = GeoValidaManager()
    
    if not manager.step_0_initialize_data():
        print("Failed to initialize data")
        return

    print("Data loaded.")
    
    # Access graph and data
    graph = manager.graph
    gdf = manager.map_generator.gdf_complete
    
    # Find Jaguarão
    # Search in gdf or graph
    jaguarao_id = None
    jaguarao_nm = ""
    
    print("Searching for Jaguarão...")
    jaguarao_candidates = []
    santa_maria_candidates = []
    
    for n, d in graph.hierarchy.nodes(data=True):
        if d.get('type') == 'municipality':
            name = d.get('name', '').lower()
            str_n = str(n)
            
            if 'jagua' in name and str(n).startswith('43'):
                print(f"Candidate RS Jagua*: {d['name']} ({n})")
                import unicodedata
                if 'jaguarao' in unicodedata.normalize('NFKD', name).encode('ASCII', 'ignore').decode('ASCII').lower():
                     jaguarao_candidates.append(n)


            
            if 'santa maria' == name:
                print(f"Candidate Santa Maria: {d['name']} ({n})")
                santa_maria_candidates.append(n)
    
    # Pick RS (starts with 43)
    jaguarao_id = next((x for x in jaguarao_candidates if str(x).startswith('43')), None)
    santa_maria_id = next((x for x in santa_maria_candidates if str(x).startswith('43')), None)

    if not jaguarao_id:
        print("Jaguarão (RS) not found!")
        return
    if not santa_maria_id:
        print("Santa Maria (RS) not found!")
        return

    print(f"Selected Jaguarão: {jaguarao_id}")
    print(f"Selected Santa Maria: {santa_maria_id}")

    
    # Check current UTP of Jaguarão
    current_utp = graph.get_municipality_utp(jaguarao_id)
    print(f"Current UTP of Jaguarão: {current_utp}")
    
    
    # 3. Analyze Flows (early)
    print("Running Flow Analysis (Step 2)...")
    manager.step_2_analyze_flows()
    df_flow = manager.analyzer.full_flow_df

    # 4. Check Neighbors
    print("\n--- Geometric Neighbors of Jaguarão ---")
    
    # SYNC GRAPH TO GET UTP_IDs
    print("Syncing map with graph...")
    manager.map_generator.sync_with_graph(graph)
    gdf = manager.map_generator.gdf_complete
    
    # Check neighbors again
    print("\n--- Neighbors of Jaguarão (After Sync) ---")
    neighbors = manager.validator.get_neighboring_utps(jaguarao_id, gdf)
    print(f"Neighbors UTP IDs: {neighbors}")
    
    
    # 5. Check "Área Operacional Lagoa Mirim"
    lagoa_id = None
    for n, d in graph.hierarchy.nodes(data=True):
         if d.get('type') == 'municipality' and 'lagoa mirim' in d.get('name', '').lower():
             print(f"Found Lagoa Mirim: {d['name']} ({n})")
             lagoa_id = n
             
    if lagoa_id:
        l_utp = graph.get_municipality_utp(lagoa_id)
        print(f"Lagoa Mirim UTP: {l_utp}")
        # Check flows from Lagoa Mirim
        if 'mun_origem' in df_flow.columns:
            l_fluxos = df_flow[df_flow['mun_origem'].astype(str) == str(lagoa_id)]
            print(f"Flows from Lagoa Mirim: {l_fluxos['viagens'].sum()}")
        
        # Check neighbors of Lagoa Mirim
        # l_neighbors = manager.validator.get_neighboring_utps(lagoa_id, gdf)
        # print(f"Lagoa Mirim Neighbors UTPs: {l_neighbors}")


    # 6. Analyze UTP 497 (Santa Maria) Membership
    print("\n--- Members of UTP 497 (Santa Maria) ---")
    sm_members = []
    if graph.hierarchy.has_node("UTP_497"):
        sm_members = list(graph.hierarchy.successors("UTP_497"))
        print(f"Count: {len(sm_members)}")
        member_names = [graph.hierarchy.nodes[m].get('name') for m in sm_members]
        print(f"Members: {member_names}")
        
        # Check if any member is a geometric neighbor of Jaguarão
        print("Checking if any Santa Maria member is a neighbor of Jaguarão...")
        is_contact = False
        jag_geom = manager.validator._safe_get_geometry(gdf, 'CD_MUN', jaguarao_id)
        buf = manager.validator._get_buffer_value(gdf)
        jag_buf = jag_geom.buffer(buf)
        
        for m in sm_members:
             m_geom = manager.validator._safe_get_geometry(gdf, 'CD_MUN', m)
             if m_geom and m_geom.intersects(jag_buf):
                 print(f"!!! MATCH FOUND: {graph.hierarchy.nodes[m].get('name')} ({m}) is in UTP 497 and borders Jaguarão!")
                 is_contact = True
        
        if not is_contact:
            print("No geometric contact found between Jaguarão and ANY member of UTP 497.")

    # 7. Analyze UTP 16 (Arroio Grande) Membership
    print("\n--- Members of UTP 16 (Arroio Grande) ---")
    if graph.hierarchy.has_node("UTP_16"):
        ag_members = list(graph.hierarchy.successors("UTP_16"))
        print(f"Count: {len(ag_members)}")
        print(f"Members: {[graph.hierarchy.nodes[m].get('name') for m in ag_members]}")


    # 8. Check Flow names (defined checks)
    print("\n--- Top Flows from Jaguarão (with Names) ---")
    
    fluxos = df_flow[df_flow['mun_origem'].astype(str) == str(jaguarao_id)]
    fluxos = fluxos.sort_values('viagens', ascending=False).head(10).copy()
    
    names = []
    utps = []
    for dest in fluxos['mun_destino']:
        d_node = graph.hierarchy.nodes.get(int(dest))
        names.append(d_node.get('name', 'Unknown') if d_node else 'Unknown')
        utps.append(graph.get_municipality_utp(int(dest)) if d_node else 'Unknown')
        
    # ... previous setup ... (keep)

    # 9. RUN FUNCTIONAL CONSOLIDATION
    print("\n==================================")
    print("RUNNING STEP 5: Functional Consolidation")
    print("==================================")
    try:
        changes = manager.step_5_consolidate_functional()
        print(f"Consolidations made: {changes}")
    except Exception as e:
        print(f"Error in step 5: {e}")
        import traceback
        traceback.print_exc()

    # 10. Check Post-Consolidation Status
    print("\n--- POST-CONSOLIDATION STATUS ---")
    
    # Check Jaguarão
    current_utp = graph.get_municipality_utp(jaguarao_id)
    d = graph.hierarchy.nodes[int(jaguarao_id)]
    print(f"Jaguarão ({jaguarao_id}): UTP={current_utp}, Name={d.get('name')}")
    
    # Check targets again
    for name, mid in targets.items():
        mid = int(mid)
        if graph.hierarchy.has_node(mid):
            utp = graph.get_municipality_utp(mid)
            d = graph.hierarchy.nodes[mid]
            print(f"{name} ({mid}): UTP={utp}, Name={d.get('name')}")
    
    # Check if Arroio Grande merged into Santa Maria?
    # Or if Jaguarão merged into Santa Maria?
    
    # 11. Run REGIC Consolidation (Step 7) just in case
    print("\n==================================")
    print("RUNNING STEP 7: Territorial Cleanup")
    print("==================================")
    try:
        changes7 = manager.step_7_territorial_cleanup()
        print(f"Consolidations made: {changes7}")
    except Exception as e:
        print(f"Error in step 7: {e}")

    print("\n--- POST-STEP 7 STATUS ---")
    current_utp = graph.get_municipality_utp(jaguarao_id)
    print(f"Jaguarão ({jaguarao_id}): UTP={current_utp}")
    
    for name, mid in targets.items():
        mid = int(mid)
        utp = graph.get_municipality_utp(mid)
        print(f"{name} ({mid}): UTP={utp}")



    
    # Check if Santa Maria is a neighbor
    # We need to know Santa Maria's UTP ID
    sm_utp = graph.get_municipality_utp(santa_maria_id)
    print(f"\nUTP of Santa Maria: {sm_utp}")
    
    is_neighbor = sm_utp in neighbors
    print(f"Is Santa Maria UTP ({sm_utp}) a neighbor? {is_neighbor}")
    
    # Check distance between Jaguarão and Santa Maria
    print(f"\nCalculating distance...")
    jag_geom = manager.validator._safe_get_geometry(gdf, 'CD_MUN', jaguarao_id)
    sm_geom = manager.validator._safe_get_geometry(gdf, 'CD_MUN', santa_maria_id)
    
    if jag_geom and sm_geom:
        # Reproject to meters
        # Assuming gdf.crs is set, otherwise default to 4326
        # The logic uses EPSG:5880
        from geopandas import GeoSeries
        s = GeoSeries([jag_geom, sm_geom], crs=gdf.crs).to_crs(epsg=5880)
        dist = s.iloc[0].distance(s.iloc[1])
        print(f"Distance Jaguarão - Santa Maria: {dist/1000:.2f} km")
    else:
        print("Could not calculate distance.")

if __name__ == "__main__":
    diagnose()
