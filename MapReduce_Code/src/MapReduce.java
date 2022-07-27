import java.io.*;
import java.util.*;

import mpi.*;

public class MapReduce {
    final static File folder = new File("E:\\An4_Nico\\An4_Nico\\APD\\APD_MapReduce\\MapReduce_Code\\src\\resources\\test-files");

    public static List<File> readFilesFromAFolder(final File folder) {
        File[] listOfFiles = folder.listFiles();
        List<File> files = new ArrayList<>();

        if (listOfFiles != null) {
            for (final File file : listOfFiles) {
                if (file.isDirectory()) {
                    readFilesFromAFolder(file);
                } else {
                    if (file.isFile()) {
                        files.add(file);
                    }
                }
            }
        }
        return files;
    }

    public static Map<String, List<String[]>> readWordsFromAFile() throws IOException {
        List<File> files = readFilesFromAFolder(folder);
        List<String[]> wordsList = new ArrayList<>();
        Map<String, List<String[]>> map = new HashMap<>();

        for (File file : files) {
            //System.out.println(file.getName());
            BufferedReader br;
            FileReader fr = new FileReader(file);
            try {
                br = new BufferedReader(fr);
                String line = br.readLine();
                while (line != null) {
                    wordsList.add(line.split("[, =?!-`ï)%~({»|_}…–¹áàæéèïñóüž)—�§¸©’“‘¢¥1234567890@\t/:;\"',*+&^$#;¿.\n@]+"));
                    line = br.readLine();
                }
                map.put(file.getName(), wordsList);
                br.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            fr.close();
        }
        return map;
    }

    public static void writeAWordToAFile(String word) {
        if (!Objects.equals(word, "")) {
            String nameFile = "E:\\An4_Nico\\An4_Nico\\APD\\APD_MapReduce\\MapReduce_Code\\src\\resources\\test-files\\temp\\" + word.toLowerCase().charAt(0) + ".txt";
            File fileOut = new File(nameFile);

            System.out.println(fileOut.getName());
            try {
                if (fileOut.exists()) { //exista
                    try {
                        FileWriter myWriter = new FileWriter(fileOut.getAbsoluteFile());
                        BufferedWriter bw = new BufferedWriter( myWriter );
                        bw.write(word);
                        bw.close();
                        System.out.println("Successfully wrote to the file.");
                    } catch (IOException e) {
                        System.out.println("An error occurred.");
                        e.printStackTrace();
                    }
                }
                else //nu exista il creez si scriu in el
                {
                    fileOut.createNewFile();
                    try {
                        FileWriter myWriter = new FileWriter(fileOut.getAbsoluteFile());
                        BufferedWriter bw = new BufferedWriter( myWriter );
                        bw.write(word);
                        bw.close();
                        System.out.println("Successfully wrote to the file.");
                    } catch (IOException e) {
                        System.out.println("An error occurred.");
                        e.printStackTrace();
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static void Mapper() throws IOException {
        Map<String, List<String[]>> map = readWordsFromAFile();

        Set keys = map.keySet();
        Iterator i = keys.iterator();
        while (i.hasNext()) {
            //System.out.println(i.next());
        }

        Collection<List<String[]>> getValues = map.values();

        for (List<String[]> words : getValues) {

            for (String[] word : words) {

                for (String w : word) {
                    writeAWordToAFile(w);
                }
            }
        }
    }

    static public void main(String[] args) throws IOException {
        MPI.Init(args);
        int my_rank = MPI.COMM_WORLD.Rank();
        Comm comm = MPI.COMM_WORLD;

        Mapper();

//        if(my_rank==0)
//        {
//            List<File> files = readFilesFromAFolder(folder);
//            List<String> worker1 = null, worker2=null, worker3=null;
//            for(int i=0;i<files.size()-1;i++)
//            {
//                if(i%2==0)
//                {
//                    worker1.add(files.get(i).getName());
//                }
//                else
//                {
//                    worker2.add(files.get(i).getName());
//                }
//            }
//
//            comm.Send(worker1,0, worker1.size(), MPI.OBJECT, 1, 99);
//            comm.Send(worker2,0, worker2.size(), MPI.OBJECT, 1, 99);
//
//            List<String> map1=null, map2=null;
//            comm.Recv(map1, 0, 1, MPI.OBJECT, 1, 99);
//            comm.Recv(map2, 0, 1, MPI.OBJECT, 1, 99);
//
//            for(String m : map2)
//            {
//                map1.add(m);
//            }
//
//            comm.Send(worker1,0, worker1.size(), MPI.OBJECT, 3, 99);
//            comm.Send(worker2,0, worker1.size(), MPI.OBJECT, 4, 99);
//        }
//        else if(my_rank==1 || my_rank==2)
//        {
//            List<String> data = null, map=null;
//            comm.Recv(data, 0, 1, MPI.OBJECT, 0, 99);
//            for(int i=0;i<data.size();i++)
//            {
//                Mapper();
//            }
//            comm.Send(map, 0, 1, MPI.OBJECT, 0, 99);
//        }
        MPI.Finalize();
    }
}
