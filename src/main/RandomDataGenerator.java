/*
*
* 
* geschrieben von Alexander Luebeck,
* 
* Das Programm (inklusive der zusaetzlich erforderlichen, Zufallsnamen enthaltenden
* Datei Datenimport.java) kann unter https://github.com/ndsvw/Neo4j-MySQL-Random-Data
* heruntergeladen werden.
* 
* Nutzung des Programms: Das Programm erzeugt Zufallspersonen und 50 Zufallsbeziehungen
* zwischen ihnen, die in 3 Dateien im Projektverzeichnis gespeichert werden. Beim Start
* ohne Parameter werden 1000 Personen erzeugt. Ist der Parameter eine Zahl, so werden
* dementsprechend viele Personen generiert.
* 
* Nutzung der Dateien (von Neo4j genutzte Dateien muessen im import-Verzeichnis der DB
* liegen):
* 		MySQL:
* 	 		LOAD DATA LOCAL INFILE '[path_to]/personen.csv'
* 			INTO TABLE Person
*			FIELDS TERMINATED BY ','
*			IGNORE 1 LINES
*			(nickname, name)
*			SET id = NULL;
*		
*			LOAD DATA LOCAL INFILE '[path_to]/following_id.csv'
*			INTO TABLE Following
*			FIELDS TERMINATED BY ','
*			IGNORE 1 LINES;
*		Neo4j:
*			USING PERIODIC COMMIT 500
*			LOAD CSV WITH HEADERS FROM "file:///personen.csv" AS csvLine
*			CREATE (p:Person { nickname: csvLine.nickname, name: csvLine.name })
*
*			USING PERIODIC COMMIT 500
*			LOAD CSV WITH HEADERS FROM "file:///following_nicknamen.csv" AS csvLine
*			MATCH (person1:Person { nickname: csvLine.p1}),
*			(person2:Person { nickname: csvLine.p2})
*			CREATE (person1)-[:follows]->(person2)
* 
* 
* Empfehlung bei einer grossen Anzahl ab Personen (ab 1 Mio.):
* Uebergabe der Parameter "-Xms4g -Xmx6g" an die Java VM zur Anpassung der Heap-Size.
* 
*/

package main;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;
import java.util.stream.Stream;

class Person
{
    private int id;

    private String name;

    private String nickname;

    public Person(int id, String vorname, String nachname)
    {
        this.id = id;
        this.name = vorname + " " + nachname;
        this.nickname = new StringBuffer(nachname.toLowerCase()).append("_").append(
                        vorname.toLowerCase()).toString();
    }

    public int getId()
    {
        return id;
    }

    public String getNickname()
    {
        return nickname;
    }

    @Override
    public String toString()
    {
        return new StringBuffer(this.nickname).append(",").append(this.name).toString();
    }
}

class Folgen
{
    private Person knoten1;

    private Person knoten2;

    public Folgen(Person knoten1, Person knoten2)
    {
        this.knoten1 = knoten1;
        this.knoten2 = knoten2;
    }

    public Person getKnoten1()
    {
        return knoten1;
    }

    public Person getKnoten2()
    {
        return knoten2;
    }

    @Override
    public String toString()
    {
        return new StringBuffer(knoten1.toString()).append(",").append(knoten2)
                        .toString();
    }
}

public class RandomDataGenerator
{
    private static int ANZAHL_KNOTEN = 1000, FOLGEN_PRO_KNOTEN = 50;

    public static void main(String[] args) throws Exception
    {
        if (args.length > 0)
        {
            ANZAHL_KNOTEN = Integer.valueOf(args[0]);
        }

        long millis = System.currentTimeMillis();

        // Daten laden
        final String[] vornamen = Stream.concat(Arrays.stream(DataImport.VORNAMEN_M),
                        Arrays.stream(DataImport.VORNAMEN_W)).distinct().toArray(
                                        String[]::new);
        final String[] nachnamen = DataImport.NACHNAMEN;

        // Personen generieren
        ArrayList<Person> generiertePersonen = new ArrayList<>();
        boolean[][] lookUpNamen = new boolean[vornamen.length][nachnamen.length];

        for (int i = 0; i < ANZAHL_KNOTEN; i++)
        {
            Random random = new Random();
            int vornameID = random.nextInt(vornamen.length);
            int nachnameID = random.nextInt(nachnamen.length);

            if (!lookUpNamen[vornameID][nachnameID])
            {
                Person person = new Person(i, vornamen[vornameID],
                                nachnamen[nachnameID]);
                generiertePersonen.add(person);
                lookUpNamen[vornameID][nachnameID] = true;
                continue;
            }

            i--;
        }

        ArrayList<Folgen> folgenList = generiereFollowings(generiertePersonen);

        writePersonen(generiertePersonen);
        writeFollowingIDs(folgenList);
        writeFollowingNicknamen(folgenList);

        System.out.println("Erfolgreich generiert: ");
        System.out.println("\t" + generiertePersonen.size() + " Personen");
        System.out.println("\t" + folgenList.size() + " Beziehungen");
        System.out.println("\tin " + (System.currentTimeMillis() - millis) + " ms");
    }

    public static ArrayList<Folgen> generiereFollowings(ArrayList<Person> personen)
    {
        ArrayList<Folgen> folgen = new ArrayList<>();
        for (int i = 0; i < ANZAHL_KNOTEN; i++)
        {
            ArrayList<Integer> endknoten = new ArrayList<>(50);
            for (int j = 0; j < FOLGEN_PRO_KNOTEN; j++)
            {
                Random random = new Random();
                int tmp = random.nextInt(ANZAHL_KNOTEN);
                if (tmp == i || endknoten.contains(tmp))
                {
                    --j;
                    continue;
                }
                endknoten.add(tmp);
                folgen.add(new Folgen(personen.get(i), personen.get(tmp)));
            }
        }
        return folgen;
    }

    public static void writePersonen(ArrayList<Person> personen)
                    throws FileNotFoundException, UnsupportedEncodingException
    {
        PrintWriter writer = new PrintWriter("personen.csv", "UTF-8");
        writer.println("nickname,name");
        for (Person person : personen)
        {
            writer.println(person.toString());
        }
        writer.close();
    }

    public static void writeFollowingIDs(ArrayList<Folgen> fList)
                    throws FileNotFoundException, UnsupportedEncodingException
    {
        PrintWriter writer = new PrintWriter("personen.csv", "UTF-8");
        writer.println("p1,p2");
        for (Folgen f : fList)
        {
            writer.println(new StringBuffer().append(f.getKnoten1().getId() + 1).append(
                            ",").append(f.getKnoten2().getId() + 1).toString());
        }
        writer.close();
    }

    public static void writeFollowingNicknamen(ArrayList<Folgen> fList)
                    throws FileNotFoundException, UnsupportedEncodingException
    {
        PrintWriter writer = new PrintWriter("personen.csv", "UTF-8");
        writer.println("p1,p2");
        for (Folgen f : fList)
        {
            writer.println(new StringBuffer(f.getKnoten1().getNickname()).append(",")
                            .append(f.getKnoten2().getNickname()).toString());
        }
        writer.close();
    }

}
