package com.bigchaindb.smartchaindb.driver;


import java.util.ArrayList;
import java.util.List;

public interface Capabilities {
	public static String PLASTIC = "Plastics_Manufacturing";
	public static String PRINTING_3D = "3D_Printing";
	public static String POCKET_MACHINING = "Pocket_machining";
	public static String MILLING = "Milling";
	public static String THREADING = "Threading";
	public static String MISC = "Miscellaneous";

	static List<String> getAll(){

		List<String> cap = new ArrayList<>();
		cap.add(Capabilities.PLASTIC);
		cap.add(Capabilities.MILLING);
		cap.add(Capabilities.THREADING);
		cap.add(Capabilities.PRINTING_3D);
		cap.add(Capabilities.POCKET_MACHINING);
		return cap;
	}

}
