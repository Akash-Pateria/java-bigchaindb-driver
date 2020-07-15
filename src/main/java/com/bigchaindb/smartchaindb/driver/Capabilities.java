package com.bigchaindb.smartchaindb.driver;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

public interface Capabilities {

	public static final String PLASTIC = "Plastics_Manufacturing";
	public static final String PRINTING_3D = "3D_Printing";
	public static final String POCKET_MACHINING = "Pocket_machining";
	public static final String MILLING = "Milling";
	public static final String TURNING = "Turning";
	public static final String DRILLING = "Drilling";
	public static final String THREADING = "Threading";
	public static final String MISC = "Miscellaneous";

	// Joining Manfacturing Process starts here
	public static final String ADHESIVE_CURING = "AdhesiveCuring";
	public static final String SOLDERING = "Soldering";
	public static final String COLD_WELDING = "ColdWelding";
	public static final String PLASTIC_WELDING = "PlasticWelding";
	public static final String METAL_WELDING = "MetalWelding";
	public static final String CLAMPING = "Clamping";
	public static final String SCREWING = "Screwing";
	public static final String SEWING = "Sewing";
	public static final String TAPING = "Taping";
	public static final String STAPLING = "Stapling";
	public static final String THREAD_RUNNING = "ThreadRunning";
	public static final String RIVETING = "Riveting";
	public static final String SPRING_FASTENING = "SpringFastening";
	public static final String TIGHTENING_FROM_SIDE = "TighteningFromSide";
	public static final String ELASTIC_DEFORMING_FIXATING = "ElasticDeformingFixating";
	public static final String PLASTIC_DEFORMING_FIXATING = "PlasticDeformingFixating";
	// Preparation starts here
	public static final String SOLDER_PASTE_APPLICATION = "SolderPasteApplication";
	public static final String UNPACKING = "Unpacking";
	public static final String LOADING = "Loading";
	public static final String FIXTURING = "Fixturing";
	public static final String ADHESIVE_APPLICATION = "AdhesiveApplication";
	// Finalization starts here
	public static final String MARKING = "Marking";
	public static final String PACKAGING = "Packaging";
	public static final String SURFACE_FINISHING = "SurfaceFinishing";
	public static final String UNLOADING = "Unloading";
	public static final String CLEANING = "Cleaning";
	// Shaping Manufacturing starts here
	public static final String MACHINING = "Machining";
	public static final String LASER_CUTTING = "LaserCutting";
	public static final String PUNCHING = "Punching";
	public static final String GRINDING = "Grinding";
	public static final String MOULDING = "Moulding";
	public static final String CASTING = "Casting";
	public static final String LASER_ADDING = "LaserAdding";
	public static final String FORMING = "Forming";
	// Non-shaping Manufacturing starts here
	public static final String HEAT_TREATMENT = "HeatTreatment";
	public static final String LOGISTIC = "Logistic";
	public static final String QUALIFYING = "Qualifying";

	static HashSet<String> getAll() {
		final HashSet<String> cap = new HashSet<>();
		cap.add(Capabilities.TURNING);
		cap.add(Capabilities.DRILLING);
		cap.add(Capabilities.MILLING);
		cap.add(Capabilities.POCKET_MACHINING);
		cap.add(Capabilities.THREADING);
		cap.add(Capabilities.PRINTING_3D);
		// cap.add(Capabilities.MISC);

		cap.add(Capabilities.ADHESIVE_CURING);
		cap.add(Capabilities.SOLDERING);
		cap.add(Capabilities.COLD_WELDING);
		cap.add(Capabilities.PLASTIC_WELDING);
		cap.add(Capabilities.METAL_WELDING);
		cap.add(Capabilities.CLAMPING);
		cap.add(Capabilities.SCREWING);
		cap.add(Capabilities.SEWING);
		cap.add(Capabilities.TAPING);
		cap.add(Capabilities.STAPLING);
		cap.add(Capabilities.THREAD_RUNNING);
		cap.add(Capabilities.RIVETING);
		cap.add(Capabilities.SPRING_FASTENING);
		cap.add(Capabilities.TIGHTENING_FROM_SIDE);
		cap.add(Capabilities.ELASTIC_DEFORMING_FIXATING);
		cap.add(Capabilities.PLASTIC_DEFORMING_FIXATING);
		cap.add(Capabilities.SOLDER_PASTE_APPLICATION);
		cap.add(Capabilities.UNPACKING);
		cap.add(Capabilities.LOADING);
		cap.add(Capabilities.FIXTURING);
		cap.add(Capabilities.ADHESIVE_APPLICATION);
		cap.add(Capabilities.MARKING);
		cap.add(Capabilities.PACKAGING);
		cap.add(Capabilities.SURFACE_FINISHING);
		cap.add(Capabilities.UNLOADING);
		cap.add(Capabilities.CLEANING);
		cap.add(Capabilities.MACHINING);
		cap.add(Capabilities.LASER_CUTTING);
		cap.add(Capabilities.PUNCHING);
		cap.add(Capabilities.GRINDING);
		cap.add(Capabilities.MOULDING);
		cap.add(Capabilities.CASTING);
		cap.add(Capabilities.LASER_ADDING);
		cap.add(Capabilities.FORMING);
		cap.add(Capabilities.HEAT_TREATMENT);
		cap.add(Capabilities.LOGISTIC);
		cap.add(Capabilities.QUALIFYING);

		return cap;
	}
}