/* ---------------------------------------------------------------
Práctica 1. 
Código fuente : gestor.c 
Grau Informàtica
Adrià Bonet Vidal 47901230G
Pedro Calero Montalbán 48253055K
---------------------------------------------------------------*/


import java.io.File;
import java.util.Collection;
import java.util.Vector;

 
abstract class MapReduce extends Thread
{
	public static final boolean DEBUG = false;	
	
	private String 	InputPath;
	private String 	OutputPath;

	private Vector<Partial> Partials = new Vector<Partial>();
	private Vector<ReduceRunner> Reducersitos = new Vector<ReduceRunner>();

	private Vector<Map> Mappers =  new Vector<Map>();
	private Vector<Reduce> Reducers =  new Vector<Reduce>();



	public MapReduce()
	{
		SetInputPath("");
		SetOutputPath("");
	}
		
	public MapReduce(String input, String output, int nReducers)
	{
		SetInputPath(input);
		SetOutputPath(output);
		SetReducers(nReducers);
	}
	
	
	private void AddMap(Map map) 
	{ 
		Mappers.add(map); 
	}
	
	private void AddReduce(Reduce reducer) 
	{ 
		Reducers.add(reducer); 
	}
	
	private void AddPartial(Partial part)     
	{
		Partials.add(part);
	}

	private void AddRed(ReduceRunner r)
	{
		Reducersitos.add(r);
	}
	
	public void SetInputPath(String path) {
		InputPath = path;
	}
	
	public void SetOutputPath(String path) {
		OutputPath = path;
	}
	

	public void SetReducers(int nReducers)
	{
		for(int x=0;x<nReducers;x++)
		{
			AddReduce(new Reduce(this, OutputPath+"/result.r"+(x+1)));
		}
	}

	// Procesa diferentes fases del framework mapreduc: split, map, suffle/merge, reduce.
	public Error Run()
	{
		if (PartialResolve(InputPath)!=Error.COk)
			Error.showError("MapReduce::Run-Error Split");
	
		if (Reduces()!=Error.COk)
			Error.showError("MapReduce::Run-Error Reduce");

		return(Error.COk);
	}



	private class Partial extends Thread{ //clase per fer Split, map i reduce
		File file;
		Map map;
		public Partial(File file, Map map){
			this.map = map;
			this.file = file;
		}

		public void run(){

/* SPLIT */
			map.ReadFileTuples(file.getAbsolutePath());
		    	

/* MAP */
	    	if (MapReduce.DEBUG) System.err.println("DEBUG::Running Map "+ map);
				map.run();
			if (map.getErr()!=Error.COk)
				Error.showError("MapReduce::Map Run error.\n");

/* SHUFFLE */
			if (MapReduce.DEBUG) map.PrintOutputs();

			for (String key : map.GetOutput().keySet())
			{		    
			    // Calcular a que reducer le corresponde está clave:
				int r = key.hashCode()%Reducers.size();
				if (r < 0) r+=Reducers.size(); 
				if (MapReduce.DEBUG) System.err.println("DEBUG::MapReduce::Suffle merge key " + key +" to reduce " + r);
	
				// Añadir todas las tuplas de la clave al reducer correspondiente.
				Reducers.get(r).AddInputKeys(key, map.GetOutput().get(key));			
			}
			
			// Eliminar todas las salidas.
			map.GetOutput().clear();



		}
	}
	
	private Error PartialResolve(String input)
	{       
		File folder = new File(input);
	
		if (folder.isDirectory()) 
		{       
		        
			File[] listOfFiles = folder.listFiles();
		        
		    for (int i = 0; i < listOfFiles.length; i++) 
		    {
		    	if (listOfFiles[i].isFile()) 
		    	{
		    		System.out.println("Processing input file " + listOfFiles[i].getAbsolutePath() + ".");
		    		Map map = new Map(this);
					AddMap(map);

					Partial part = new Partial(listOfFiles[i], map);
		    		AddPartial(part);		//afegim al vector de Partials el objecte creat per poder fer joins facilment
		    		part.start();
		    	}
		    	else if (listOfFiles[i].isDirectory()) {
		    		System.out.println("Directory " + listOfFiles[i].getName());
		    	}
		    }

		    for(Partial p : Partials){
		    	try{
		    		p.join();			//Join split map shuffle
		    	}catch(InterruptedException e){
		    		e.printStackTrace();

		    	}
		    }
		}
		else 
		{
			File file = folder;
			Map map = new Map(this);			
			AddMap(map);
			System.out.println("Processing input file " + folder.getAbsolutePath() + ".");
			Partial part = new Partial(file, map);
			AddPartial(part);				//Cas de tenir 1 fitxer = 1 thread 1 join
			part.start();

			try{
	    		part.join();
	    	}catch(InterruptedException e){
	    		e.printStackTrace();

	    	}
    		        
		}

		return(Error.COk);
	}

	
	class ReduceRunner extends Thread{ //Classe que farà el Reduce
		Reduce reduce;
		public ReduceRunner(Reduce r){
			reduce = r;
			
		}

		public void run(){
			reduce.run();
			if (reduce.getErr()!=Error.COk)
				Error.showError("MapReduce::Reduce Run error.\n");
		}
	}	

	
	public Error Map(Map map, MapInputTuple tuple)
	{
		System.err.println("MapReduce::Map -> ERROR map must be override.");
		return(Error.CError);
	}
	

	private Error Reduces()
	{
		
		for(Reduce reduce : Reducers)
		{
			ReduceRunner red = new ReduceRunner(reduce);
			AddRed(red);											//Threads REDUCE (1 per cada Reduce)
			red.start();
		}

		for(ReduceRunner red : Reducersitos){
			try{
				red.join();
			} catch (InterruptedException e){							//join REDUCE
				e.printStackTrace();
			}
		}

		return(Error.COk);
	}
	
	public Error Reduce(Reduce reduce, String key, Collection<Integer> values)
	{
		System.err.println("MapReduce::Reduce  -> ERROR Reduce must be override.");
		return(Error.CError);
	}
}




