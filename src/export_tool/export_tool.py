import os
import psycopg2
import pandas as pd

# BUFR/GRIB descriptor mappings
BUFR_GRIB_MAPPINGS = {
    "temperature": {
        "bufr": {"descriptor": 120101, "key": "airTemperature"},
        "grib": {"discipline": 0, "category": 0, "number": 0, "abbrev": "TMP"},
    },
    "pressure": {
        "bufr": {"descriptor": 10004, "key": "pressure"},
        "grib": {"discipline": 0, "category": 3, "number": 0, "abbrev": "PRES"},
    },
    "humidity": {
        "bufr": {"descriptor": 13003, "key": "relativeHumidity"},
        "grib": {"discipline": 0, "category": 1, "number": 1, "abbrev": "RH"},
    },
    "wind_direction": {
        "bufr": {"descriptor": 11001, "key": "windDirection"},
        "grib": {"discipline": 0, "category": 2, "number": 0, "abbrev": "WDIR"},
    },
    "wind_speed": {
        "bufr": {"descriptor": 11002, "key": "windSpeed"},
        "grib": {"discipline": 0, "category": 2, "number": 1, "abbrev": "WIND"},
    },
    "solar_radiation": {
        "bufr": {"descriptor": 14021, "key": "solarRadiation"},
        "grib": {"discipline": 0, "category": 4, "number": 1, "abbrev": "NSWRT"},
    },
    "latitude": {
        "bufr": {"descriptor": 5001, "key": "latitude"},
        "grib": {"discipline": 0, "category": 191, "number": 192, "abbrev": "NLAT"},
    },
    "longitude": {
        "bufr": {"descriptor": 6001, "key": "longitude"},
        "grib": {"discipline": 0, "category": 191, "number": 193, "abbrev": "ELON"},
    },
    "altitude": {
        "bufr": {"descriptor": 7002, "key": "height"},
        # No GRIB classification found
    },
    "visibility": {
        "bufr": {"descriptor": 20001, "key": "visibility"},  # Horizontal
        "grib": {"discipline": 0, "category": 19, "number": 0, "abbrev": "VIS"},
    },
    "aerosol_concentration": {
        # No BUFR equivalent
        "grib": {"discipline": 0, "category": 20, "number": 59, "abbrev": "ANCON"},
    },
    "mass_density": {
        # No BUFR equivalent
        "grib": {"discipline": 0, "category": 20, "number": 0, "abbrev": "MASSDEN"},
    },
}


class ExportTool:
    def __init__(self, db_params):
        self.db_params = db_params

    def query_db(self, query):
        """Execute SQL query and return DataFrame"""
        try:
            with psycopg2.connect(**self.db_params) as conn:
                df = pd.read_sql_query(query, conn)
            return df
        except Exception as e:
            print(f"❌ Database query failed: {e}")
            raise

    def write_file(self, df, format_type, filepath):
        """Write DataFrame to file in specified format"""
        try:
            if format_type == "csv":
                df.to_csv(filepath, index=False)
            elif format_type == "json":
                df.to_json(filepath, orient="records", lines=True)
            elif format_type == "excel":
                df.to_excel(filepath, index=False)
            elif format_type == "bufr":
                self.write_bufr(df, filepath)
            elif format_type == "grib":
                self.write_grib(df, filepath)
            else:
                raise ValueError(f"Unsupported format: {format_type}")
            
            print(f"✅ Successfully wrote {format_type.upper()} to {filepath}")
            
        except Exception as e:
            print(f"❌ Failed to write {format_type.upper()} file: {e}")
            raise

    def write_bufr(self, df, filepath):
        """Write DataFrame to BUFR format (requires eccodes library)"""
        try:
            from eccodes import (
                codes_bufr_new_from_samples,
                codes_set,
                codes_write,
                codes_release,
            )
            
            with open(filepath, "wb") as f:
                for _, row in df.iterrows():
                    bufr = codes_bufr_new_from_samples("BUFR4")

                    descriptors = []
                    for field, mapping in BUFR_GRIB_MAPPINGS.items():
                        if "bufr" in mapping and field in df.columns:
                            descriptors.append(mapping["bufr"]["descriptor"])

                    if not descriptors:
                        continue  # skip if no mappable fields

                    codes_set(bufr, "unexpandedDescriptors", descriptors)

                    for field, mapping in BUFR_GRIB_MAPPINGS.items():
                        if "bufr" in mapping and field in df.columns:
                            try:
                                value = float(row[field])
                                codes_set(bufr, mapping["bufr"]["key"], value)
                            except (ValueError, TypeError):
                                continue

                    codes_set(bufr, "pack", 1)
                    codes_write(bufr, f)
                    codes_release(bufr)
                    
        except ImportError:
            print("⚠️ eccodes library not available. Skipping BUFR export.")
            print("   Install with: pip install eccodes-python")
            return
        except Exception as e:
            print(f"❌ BUFR writing failed: {e}")
            raise

    def write_grib(self, df, filepath):
        """Write DataFrame to GRIB format (requires eccodes library)"""
        try:
            from eccodes import (
                codes_grib_new_from_samples,
                codes_set,
                codes_write,
                codes_release,
            )
            
            with open(filepath, "wb") as f:
                for field, mapping in BUFR_GRIB_MAPPINGS.items():
                    if "grib" not in mapping or field not in df.columns:
                        continue

                    for _, row in df.iterrows():
                        grib = codes_grib_new_from_samples("GRIB2")

                        codes_set(grib, "discipline", mapping["grib"]["discipline"])
                        codes_set(grib, "parameterCategory", mapping["grib"]["category"])
                        codes_set(grib, "parameterNumber", mapping["grib"]["number"])

                        # Optional: add timestamp if available
                        if "timestamp" in row:
                            try:
                                if hasattr(row["timestamp"], "strftime"):
                                    codes_set(grib, "dataDate", int(row["timestamp"].strftime("%Y%m%d")))
                                    codes_set(grib, "dataTime", int(row["timestamp"].strftime("%H%M")))
                            except:
                                pass

                        # Set coordinates if available
                        if "latitude" in row:
                            try:
                                codes_set(grib, "latitudeOfFirstGridPointInDegrees", float(row["latitude"]))
                            except:
                                pass
                        if "longitude" in row:
                            try:
                                codes_set(grib, "longitudeOfFirstGridPointInDegrees", float(row["longitude"]))
                            except:
                                pass

                        try:
                            codes_set(grib, "values", [float(row[field])])
                            codes_write(grib, f)
                        except:
                            continue

                        codes_release(grib)
                        
        except ImportError:
            print("⚠️ eccodes library not available. Skipping GRIB export.")
            print("   Install with: pip install eccodes-python")
            return
        except Exception as e:
            print(f"❌ GRIB writing failed: {e}")
            raise
