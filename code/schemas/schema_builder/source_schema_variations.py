# Define baseline schemas for exact matches
DEFAULT_SCHEMAS_GPR = {
    # Group 1: Base Schema GPR and Its Variations
    # Base Schema GPR focuses on common columns for Ground Penetrating Radar (GPR) data
    "Schema 1": {
        # Base schema includes essential GPR data columns common across different GPR datasets
        "Base Schema 1": {
            'columns': {
                "Scan", "Dist.(ft)", "MP", "Lat(°)", "Long(°)", 
                "Layer 1 Name", "Layer 1 Depth(in)", "Layer 2 Name", "Layer 2 Depth(in)"
            }
        },
        # Variation GPR-A adds user-specific marks and time measurements for Layer 1 and Layer 2
        "Variation 1A": {
            'columns': { 
                "User Mark", "Layer 1 2-Way Time", "Layer 2 2-Way Time"
            }
        },
        # Variation GPR-B adds additional layers (Layer 3) with depth and dielectric properties (Diel.)
        "Variation 1B": {
            'columns': {
                "Time", "Layer 1 Layer Diel.", "Layer 2 Layer Diel.", 
                "Layer 3 Name", "Layer 3 Depth(in)", "Layer 3 Layer Diel."
            }
        }
    },
}


DEFAULT_SCHEMAS_PAVEMENT = {
    # Group 1: Base Schema 1 and Its Variations
    # Base Schema 1 focuses on common geospatial and depth measurements (Surface and Bottom AC, PCC Depth, and Thickness)
    "Schema 1": {
        # Base schema includes essential geospatial columns and core depth-related measurements
        "Base Schema 1": {
            'columns': {
                "Lattitude", "Longitude", "Scan", 
                "Surface AC Depth (in.)", "PCC Depth (in.)",
                "Bottom AC Depth (in.)", "Surface AC Thickness (in.)", 
                "PCC Thickness (in.)", "Bottom AC Thickness (in.)"
            }
        },
        # Variation 1A adds the "Distance from Start of Ramp (ft)" to measure depth relative to ramp positioning
        "Variation 1A": {
            'columns': { 
                "Distance from Start of Ramp (ft)"
            }
        },
        # Variation 1B introduces "MP" (Milepost) for road segmentation or localization
        "Variation 1B": {
            'columns': { 
                "MP"
            }
        }
    },

    # Group 2: Base Schema 2 and Its Variations
    # Base Schema 2 deals with depth measurements, particularly for AC, PCC, and base layers, with Milepost as a reference
    "Schema 2": {
        # Base schema focuses on AC (Asphalt Concrete) and PCC (Portland Cement Concrete) depth measurements using Milepost (MP)
        "Base Schema 2": {
            'columns': {
                "Lattitude", "Longitude", "Scan", "MP", 
                "AC Depth (in.)", "PCC Depth (in.)",
            }
        },
        # Variation 2A adds "Base Layer Depth (in.)" to consider the depth of underlying base materials
        "Variation 2A": {
            'columns': { 
                "Base Layer Depth (in.)"
            }
        },
        # Variation 2B includes both "Base Layer Depth (in.)" and "Base Thickness (in)"
        "Variation 2B": {
            'columns': { 
                "Base Layer Depth (in.)",
                "Base Thickness (in)"
            }
        }
    },

    # Group 3: Base Schema 3 and Its Variations (Schemas with "MP")
    # Base Schema 3 focuses on PCC depth measurements with "MP" and additional contextual columns
    "Schema 3": {
        "Base Schema 3": {
            'columns': {
                "Lattitude", "Longitude", "Scan", "MP", "PCC Depth (in.)"
            }
        },
        # Variations with "MP"
        "Variation 3A": {
            'columns': { 
                "AC Depth (in.)", "AC Thickness (in.)", 
                "Base Layer 2 Depth (in.)"
            }
        },
        "Variation 3B": {
            'columns': { 
                "Base Layer 1 Depth (in.)", "Base Layer 2 Depth (in.)"
            }
        },
        "Variation 3C": {
            'columns': { 
                "AC Layer 1 (in.)", "AC Layer 2 (in.)", 
                "Total Depth AC (in.)", "Bottom Base Layer Depth (in.)", 
                "PCC Thickness (in.)"
            }
        }
    },

    # Group 4: Base Schema 4 and Its Variations (Schemas without "MP")
    # Base Schema 4 focuses on PCC depth measurements without "MP" and additional contextual columns
    "Schema 4": {
        # Base schema includes geospatial data, ramp distance, and PCC depth measurements without "MP"
        "Base Schema 4": {
            'columns': {
                "Lattitude", "Longitude", "Scan", "PCC Depth (in.)", "Distance from Start of Ramp (ft)"
            }
        },
        # Variations with different depth and thickness measurements related to ramp distance
        "Variation 4A": {
            'columns': { 
                "Base Layer 1 Depth (in.)", "Base Layer 2 Depth (in.)"
            }
        },
        "Variation 4B": {
            'columns': { 
                "AC Depth (in.)", "AC Thickness (in.)", "Base Layer 2 Depth (in.)"
            }
        },
        "Variation 4C": {
            'columns': { 
                "AC Depth (in.)", "Base Layer Depth (in.)"
            }
        },
        "Variation 4D": {
            'columns': { 
                "AC Depth (in.)", "Base Layer Depth (in.)", "AC Thickness (in.)"
            }
        }
    },

    # Group 5: Base Schema 5 and Its Variations (Schemas with a naming variation)
    # Base Schema 5 focuses on similar data as Schema 4 but with a different naming convention for ramp distance
    "Schema 5": {
        # Base schema includes geospatial data, total AC depth, and PCC depth measurements with a naming variation for ramp distance
        "Base Schema 5": {
            'columns': {
                "Lattitude", "Longitude", "Scan", "PCC Depth (in.)", "Distance from Ramp Start (ft)"
            }
        },
        # Variation 5A focuses on total AC and PCC depth along with the modified ramp distance name
        "Variation 5A": {
            'columns': { 
                "Total AC Depth (in.)"
            }
        }
    },

    # Group 6: Base Schema 6 and Its Variations
    # Base Schema 6 combines basic geospatial data with Milepost (MP)
    "Schema 6": {
        # Base schema is minimal, including only geospatial data and MP for localization
        "Base Schema 6": {
            'columns': {
                "Lattitude", "Longitude", "Scan", "MP"
            }
        },
        # Variation 6A includes total AC depth and base layer depth, useful for comprehensive pavement thickness analysis
        "Variation 6A": {
            'columns': { 
                "Total AC Depth (in.)", "Base Layer Depth (in.)"
            }
        },
        # Variation 6B adds all relevant layers and depths for comprehensive pavement analysis
        "Variation 6B": {
            'columns': {
                "AC Layer 1 (in.)", "AC Layer 2 (in.)", 
                "Total Depth AC (in.)", "Bottom Base Layer Depth (in.)"
            }
        }
    }
}
