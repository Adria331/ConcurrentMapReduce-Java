/* ---------------------------------------------------------------
Práctica 1. 
Código fuente : gestor.c 
Grau Informàtica
Adrià Bonet Vidal 47901230G
Pedro Calero Montalbán 48253055K
---------------------------------------------------------------*/

public class WordCount 
{

	public static void main(String[] args) 
	{
	
		WordCountMR wc = new WordCountMR(args);

		wc.Run();
	
		System.exit(0);
	}

}
