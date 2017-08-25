#!/usr/bin/env python3
"""
Data Lineage Extractor
Extracts metadata from various sources to build lineage graphs
"""

import xml.etree.ElementTree as ET
import json
import re
from pathlib import Path
from typing import Dict, List, Set, Tuple
import logging
from dataclasses import dataclass
from neo4j import GraphDatabase

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class DataAsset:
    """Represents a data asset in the lineage"""
    id: str
    name: str
    type: str  # table, view, file, etc.
    schema: str = None
    database: str = None
    columns: List[str] = None

@dataclass
class DataTransformation:
    """Represents a transformation between data assets"""
    id: str
    name: str
    type: str  # ssis_package, sql_query, python_script, etc.
    source_assets: List[str] = None
    target_assets: List[str] = None
    transformation_logic: str = None

class SSISLineageExtractor:
    """Extract lineage from SSIS packages"""
    
    def __init__(self):
        self.assets = {}
        self.transformations = {}
    
    def extract_from_dtsx(self, dtsx_path: str) -> Tuple[Dict, Dict]:
        """Extract lineage from SSIS .dtsx file"""
        logger.info(f"Extracting lineage from {dtsx_path}")
        
        try:
            tree = ET.parse(dtsx_path)
            root = tree.getroot()
            
            # Define namespaces
            namespaces = {
                'DTS': 'www.microsoft.com/SqlServer/Dts',
                'pipeline': 'www.microsoft.com/sqlserver/dts/tasks/sqltask'
            }
            
            package_name = Path(dtsx_path).stem
            
            # Extract data flow tasks
            self._extract_data_flows(root, package_name, namespaces)
            
            # Extract SQL tasks
            self._extract_sql_tasks(root, package_name, namespaces)
            
            # Extract connection managers
            self._extract_connections(root, namespaces)
            
            return self.assets, self.transformations
            
        except Exception as e:
            logger.error(f"Error extracting from {dtsx_path}: {e}")
            return {}, {}
    
    def _extract_data_flows(self, root, package_name, namespaces):
        """Extract data flow components"""
        data_flows = root.findall('.//DTS:Executable[@DTS:ExecutableType="Microsoft.Pipeline"]', namespaces)
        
        for df in data_flows:
            df_name = df.get('{www.microsoft.com/SqlServer/Dts}ObjectName', 'Unknown')
            
            # Find sources
            sources = df.findall('.//component[@componentClassID="Microsoft.OLEDBSource"]')
            for source in sources:
                source_name = source.get('name', 'Unknown Source')
                table_name = self._extract_table_name(source)
                
                asset_id = f"{package_name}_{source_name}"
                self.assets[asset_id] = DataAsset(
                    id=asset_id,
                    name=table_name or source_name,
                    type="source_table",
                    schema="dbo"  # Default schema
                )
            
            # Find destinations
            destinations = df.findall('.//component[@componentClassID="Microsoft.OLEDBDestination"]')
            for dest in destinations:
                dest_name = dest.get('name', 'Unknown Destination')
                table_name = self._extract_table_name(dest)
                
                asset_id = f"{package_name}_{dest_name}"
                self.assets[asset_id] = DataAsset(
                    id=asset_id,
                    name=table_name or dest_name,
                    type="target_table",
                    schema="dbo"
                )
            
            # Create transformation
            trans_id = f"{package_name}_{df_name}"
            self.transformations[trans_id] = DataTransformation(
                id=trans_id,
                name=df_name,
                type="ssis_dataflow",
                source_assets=[a.id for a in self.assets.values() if a.type == "source_table"],
                target_assets=[a.id for a in self.assets.values() if a.type == "target_table"]
            )
    
    def _extract_sql_tasks(self, root, package_name, namespaces):
        """Extract SQL Task components"""
        sql_tasks = root.findall('.//DTS:Executable[@DTS:ExecutableType="Microsoft.ExecuteSQLTask"]', namespaces)
        
        for task in sql_tasks:
            task_name = task.get('{www.microsoft.com/SqlServer/Dts}ObjectName', 'Unknown SQL Task')
            
            # Try to extract SQL statement
            sql_statement = self._extract_sql_statement(task)
            
            if sql_statement:
                # Parse SQL to find tables
                source_tables, target_tables = self._parse_sql_tables(sql_statement)
                
                # Create assets
                for table in source_tables:
                    asset_id = f"{package_name}_{table}"
                    self.assets[asset_id] = DataAsset(
                        id=asset_id,
                        name=table,
                        type="source_table"
                    )
                
                for table in target_tables:
                    asset_id = f"{package_name}_{table}"
                    self.assets[asset_id] = DataAsset(
                        id=asset_id,
                        name=table,
                        type="target_table"
                    )
                
                # Create transformation
                trans_id = f"{package_name}_{task_name}"
                self.transformations[trans_id] = DataTransformation(
                    id=trans_id,
                    name=task_name,
                    type="sql_task",
                    transformation_logic=sql_statement[:500]  # Truncate for storage
                )
    
    def _extract_connections(self, root, namespaces):
        """Extract connection managers"""
        connections = root.findall('.//DTS:ConnectionManager', namespaces)
        
        for conn in connections:
            conn_name = conn.get('{www.microsoft.com/SqlServer/Dts}ObjectName', 'Unknown')
            conn_string = conn.get('{www.microsoft.com/SqlServer/Dts}ConnectionString', '')
            
            # Extract database name from connection string
            db_match = re.search(r'Initial Catalog=([^;]+)', conn_string)
            if db_match:
                database = db_match.group(1)
                logger.info(f"Found connection to database: {database}")
    
    def _extract_table_name(self, component):
        """Extract table name from component"""
        # Look for table name in properties
        for prop in component.findall('.//property'):
            if prop.get('name') in ['OpenRowset', 'TableOrViewName']:
                return prop.text
        return None
    
    def _extract_sql_statement(self, task):
        """Extract SQL statement from SQL task"""
        # This is a simplified extraction - real implementation would be more complex
        for prop in task.findall('.//property'):
            if 'SQL' in prop.get('name', ''):
                return prop.text
        return None
    
    def _parse_sql_tables(self, sql: str) -> Tuple[Set[str], Set[str]]:
        """Parse SQL to extract source and target tables"""
        sql_upper = sql.upper()
        
        # Find SELECT statements (sources)
        from_pattern = r'FROM\s+(\w+(?:\.\w+)?)'
        join_pattern = r'JOIN\s+(\w+(?:\.\w+)?)'
        
        source_tables = set()
        source_tables.update(re.findall(from_pattern, sql_upper))
        source_tables.update(re.findall(join_pattern, sql_upper))
        
        # Find INSERT/UPDATE/DELETE statements (targets)
        insert_pattern = r'INSERT\s+INTO\s+(\w+(?:\.\w+)?)'
        update_pattern = r'UPDATE\s+(\w+(?:\.\w+)?)'
        delete_pattern = r'DELETE\s+FROM\s+(\w+(?:\.\w+)?)'
        
        target_tables = set()
        target_tables.update(re.findall(insert_pattern, sql_upper))
        target_tables.update(re.findall(update_pattern, sql_upper))
        target_tables.update(re.findall(delete_pattern, sql_upper))
        
        return source_tables, target_tables

class LineageGraphBuilder:
    """Build lineage graph in Neo4j"""
    
    def __init__(self, uri: str, user: str, password: str):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
    
    def close(self):
        self.driver.close()
    
    def create_lineage_graph(self, assets: Dict, transformations: Dict):
        """Create lineage graph from extracted metadata"""
        with self.driver.session() as session:
            # Clear existing data
            session.run("MATCH (n) DETACH DELETE n")
            
            # Create asset nodes
            for asset in assets.values():
                session.run("""
                    CREATE (a:Asset {
                        id: $id,
                        name: $name,
                        type: $type,
                        schema: $schema,
                        database: $database
                    })
                """, 
                id=asset.id,
                name=asset.name,
                type=asset.type,
                schema=asset.schema,
                database=asset.database
                )
            
            # Create transformation nodes
            for trans in transformations.values():
                session.run("""
                    CREATE (t:Transformation {
                        id: $id,
                        name: $name,
                        type: $type,
                        logic: $logic
                    })
                """,
                id=trans.id,
                name=trans.name,
                type=trans.type,
                logic=trans.transformation_logic
                )
                
                # Create relationships
                if trans.source_assets:
                    for source_id in trans.source_assets:
                        session.run("""
                            MATCH (a:Asset {id: $source_id})
                            MATCH (t:Transformation {id: $trans_id})
                            CREATE (a)-[:FEEDS_INTO]->(t)
                        """,
                        source_id=source_id,
                        trans_id=trans.id
                        )
                
                if trans.target_assets:
                    for target_id in trans.target_assets:
                        session.run("""
                            MATCH (t:Transformation {id: $trans_id})
                            MATCH (a:Asset {id: $target_id})
                            CREATE (t)-[:PRODUCES]->(a)
                        """,
                        trans_id=trans.id,
                        target_id=target_id
                        )
    
    def get_lineage_for_asset(self, asset_name: str) -> Dict:
        """Get complete lineage for a specific asset"""
        with self.driver.session() as session:
            result = session.run("""
                MATCH path = (source:Asset)-[:FEEDS_INTO*..10]->(target:Asset {name: $asset_name})
                RETURN path
                UNION
                MATCH path = (source:Asset {name: $asset_name})-[:PRODUCES*..10]->(target:Asset)
                RETURN path
            """, asset_name=asset_name)
            
            paths = []
            for record in result:
                path_data = []
                for node in record["path"].nodes:
                    path_data.append({
                        "id": node["id"],
                        "name": node["name"],
                        "type": node.labels
                    })
                paths.append(path_data)
            
            return {"asset": asset_name, "lineage_paths": paths}

def main():
    """Main lineage extraction process"""
    logger.info("Starting Data Lineage Extraction...")
    
    # Initialize extractors
    ssis_extractor = SSISLineageExtractor()
    
    # Extract from SSIS packages
    ssis_packages = [
        "RealWorldETL/RealWorldETL/01_CustomerDataLoad.dtsx",
        "RealWorldETL/RealWorldETL/02_ProductDataLoad.dtsx",
        "RealWorldETL/RealWorldETL/03_SalesDataLoad.dtsx"
    ]
    
    all_assets = {}
    all_transformations = {}
    
    for package in ssis_packages:
        if Path(package).exists():
            assets, transformations = ssis_extractor.extract_from_dtsx(package)
            all_assets.update(assets)
            all_transformations.update(transformations)
    
    # Build graph
    graph_builder = LineageGraphBuilder("bolt://localhost:7687", "neo4j", "password")
    
    try:
        graph_builder.create_lineage_graph(all_assets, all_transformations)
        logger.info("Lineage graph created successfully")
        
        # Example: Get lineage for a specific table
        lineage = graph_builder.get_lineage_for_asset("DimCustomer")
        print(json.dumps(lineage, indent=2))
        
    finally:
        graph_builder.close()

if __name__ == "__main__":
    main()