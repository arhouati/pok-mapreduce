package pok.mapreduce.txt.tools;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.Normalizer;
import java.util.HashMap;
import java.util.Map;

public class PreProcessador {
	
    Map<String, String> emoticons = new HashMap<>();

    public PreProcessador() {
        geraDicionarioEmoticons();
    }

    public String cleanup(String s){
      
        s = removerUrl(s);
        s = removerUrlRegex(s);
        s = removerUrlPicTwitter(s);
        s = substituirEmoticons(s);
        s = removerCaracteresEspeciaisEConsulta(s);
        s = substituirLetrasRepetidas(s);
        s = removerEspacos(s);  
        
        return s;
    
    }

    private void geraDicionarioEmoticons(){
        try {
            try (BufferedReader arq = new BufferedReader(new FileReader("resources/emoticons.txt"))) {
                while(arq.ready()){
                    String linha = arq.readLine();
                    linha = linha.toLowerCase();

                    String[] values = linha.split(",");
                    emoticons.put(values[0], values[1]);
                }
                System.out.println("GERAR EMOTICONS OK\n\n");
            }
        } catch(IOException e) {
            System.out.println("!!! EMOTICONS NOT OK\n\n");
            System.exit(0);
        }
    }


    public String substituirEmoticons(String s){
        for (String key : emoticons.keySet()) {
            if (s.contains(" "+key+" ")) {
                s = s.replace(" "+key+" ", " "+emoticons.get(key)+" ");
            }
        }
        return s;
    } 

    public static String removerUrl(String s){
        String aux ="";
        if (s.contains("http")) {
            if (s.contains("…")) {
                Integer http = s.indexOf("http");
                if (s.indexOf("…", http) != -1) {
                    aux = s.substring(s.indexOf("http"), s.indexOf("…", http)+1);
                    s = s.replace(aux, " ");
                }
            }
        }
        s = s.replaceAll("@\\s*(\\w+)", " ");
        return s;
    }

    public static String removerUrlRegex(String s){
        s = s.replaceAll("https?://.*?\\s+", "INICIO_LINK");
        if (s.contains("INICIO_LINK")) {
            String aux = s.substring(s.indexOf("INICIO_LINK"));
            s = s.replace(aux, " ");
        }
        return s;
    }

    public static String removerUrlPicTwitter(String s){
        if (s.contains("pic.twitter.com")) {
            String aux = s.substring(s.indexOf("pic.twitter.com"));
            s = s.replace(aux, " ");
        }
        return s;
    }

    public static String removerCaracteresEspeciaisEConsulta(String s) {
        s = s.replaceAll("[^\\p{ASCII}]", "");
        s = s.replace( "/" , " ");
        s = s.replace( "-" , " ");
        s = s.replace( "!" , " ");
        s = s.replace( "[" , " ");
        s = s.replace( "]" , " ");
        s = s.replace( "\"" , " ");
        s = s.replace( "#" , " ");
        s = s.replace( ":" , " ");
        s = s.replace( "&", " ");
        s = s.replace( "=", " ");
        s = s.replace( "|", " ");
        s = s.replace( ">", " ");
        s = s.replace( "<", " ");
        s = s.replace( "*", " ");
        s = s.replace( "_", " ");
        s = s.replace( "%", " ");
        s = s.replace( "@", " ");
        s = s.replace( "\\", " ");
        s = s.replace( "{", " ");
        s = s.replace( "}", " ");
        s = s.replace( "^", " ");
        s = s.replace( "$", " ");
        s = s.replace( "+", " ");
        s = s.replace( "*", " ");
        s = s.replace( "~", " ");
        s = s.replace( "RT " , "");

        //s = Normalizer.normalize(s, Normalizer.Form.NFD);

        return s;
    }

    public static String removerTermoConsulta(String s, String consulta) {
        s = s.replace(consulta, " ");

        return s;
    }

    public static String substituirLetrasRepetidas(String s) {
        s = s.replaceAll ("(\\w)\\1{2,}", "$1"); 
        return s;
    }

    public static String removerEspacos(String s) {
        s = s.replaceAll("\\s{2,}", " "); 
        return s;
    }
}