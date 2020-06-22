package com.bigchaindb.smartchaindb.driver;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

public interface Capabilities {

	public static final String PLASTIC = "Plastics_Manufacturing";
	public static final String PRINTING_3D = "3D_Printing";
	public static final String POCKET_MACHINING = "Pocket_machining";
	public static final String MILLING = "Milling";
	public static final String THREADING = "Threading";
	public static final String MISC = "Miscellaneous";

	// public static final String CLAMPING = "Miscellaneous";
	// public static final String SEWING = "Miscellaneous";
	// public static final String SCREWING = "Miscellaneous";
	// public static final String RIVETING = "Miscellaneous";
	// public static final String TAPING = "Miscellaneous";
	// public static final String STAPLING = "Miscellaneous";
	// public static final String NAILING = "Miscellaneous";
	// public static final String THREADRUNNING = "Miscellaneous";
	// public static final String SPRINGFASTENING = "Miscellaneous";
	// public static final String TIGHTENING_FROM_SIDE = "Miscellaneous";

	// Joining Manfacturing Process starts here
	public static final String ADHESIVE_CURING = "Miscellaneous";
	public static final String SOLDERING = "Miscellaneous";
	public static final String COLD_WELDING = "Miscellaneous";
	public static final String PLASTIC_WELDING = "Miscellaneous";
	public static final String LASER_WELDING = "Miscellaneous";
	public static final String ARC_WELDING = "Miscellaneous";
	public static final String RESISTANCE_WELDING = "Miscellaneous";
	public static final String FASTENING = "Miscellaneous";
	public static final String ELASTIC_DEFORMING_FIXATING = "Miscellaneous";
	public static final String PLASTIC_DEFORMING_FIXATING = "Miscellaneous";
	// Joining ends here
	// Preparation starts here
	public static final String SOLDER_PASTE_APPLICATION = "Miscellaneous";
	public static final String UNPACKING = "Miscellaneous";
	public static final String LOADING = "Miscellaneous";
	public static final String FIXTURING = "Miscellaneous";
	public static final String ADHESIVE_APPLICATION = "Miscellaneous";
	// Preparation starts here
	// Finalization starts here
	public static final String MARKING = "Miscellaneous";
	public static final String PACKAGING = "Miscellaneous";
	public static final String SURFACE_FINISHING = "SurfaceFinishing";
	public static final String UNLOADING = "Miscellaneous";
	public static final String CLEANING = "Miscellaneous";
	// Finalization ends here
	// Shaping Manufacturing starts here
	public static final String MACHINING = "Machining";
	public static final String LASER_CUTTING = "Miscellaneous";
	public static final String PUNCHING = "Miscellaneous";
	public static final String GRINDING = "Miscellaneous";
	public static final String MOULDING = "Moulding";
	public static final String CASTING = "Miscellaneous";
	public static final String LASER_ADDING = "Miscellaneous";
	public static final String FORMING = "Miscellaneous";
	// Shaping Manufacturing ends here
	// Non-shaping Manufacturing starts here
	public static final String HEAT_TREATMENT = "Miscellaneous";
	// Non-shaping Manufacturing ends here

	static HashSet<String> getAll() {
		HashSet<String> cap = new HashSet<>();
		cap.add(Capabilities.PLASTIC);
		cap.add(Capabilities.MILLING);
		cap.add(Capabilities.THREADING);
		cap.add(Capabilities.PRINTING_3D);

		cap.add(Capabilities.MACHINING);
		cap.add(Capabilities.SURFACE_FINISHING);
		return cap;
	}
}