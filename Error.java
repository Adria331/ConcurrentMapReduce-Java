/* ---------------------------------------------------------------
Práctica 1. 
Código fuente : gestor.c 
Grau Informàtica
Adrià Bonet Vidal 47901230G
Pedro Calero Montalbán 48253055K
---------------------------------------------------------------*/

public enum Error {
	COk, CError, CErrorOpenInputDir, CErrorOpenInputFile, CErrorReadingFile, 
	CErrorOpenOutputFile;


	public static void showError(String message) 
	{ 
		System.err.println(message); 
		System.exit(1); 
	}
}