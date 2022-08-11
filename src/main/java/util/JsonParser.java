package util;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.util.Iterator;

public class JsonParser {
    public String value = "";
    public String key = "";
    public Boolean search = false;

    public String parse(String gStr, String gKey)
    {
        this.key = gKey;
        this.value = "";

        try
        {
            searchInner(gStr);
        }
        catch(Exception ex)
        {
            System.out.println(ex.getMessage());
        }
        finally
        {
            return this.value;
        }
    }

    public String searchInner(String gStr)
    {
        String retVal = "";
        try
        {
            JSONParser parser = new JSONParser();
            JSONObject obj = (JSONObject)parser.parse(gStr);

            if (obj.containsKey(this.key))
            {
                //if the root json object has the key
                //
                retVal = obj.get(this.key).toString();

                if (value.equalsIgnoreCase(""))
                {
                    //set the value of the class if not set before
                    value = retVal.replaceAll("[\\[\\]]+", "");
                }
                else
                {
                    //add the value of the class
                    //if there is more than one apperance of the same key
                    //resolve the csv within the map
                    value = value + "," + retVal.replaceAll("[\\[\\]]+", "");
                }
            }
            else
            {
                //if the root object doesnt have the filter key
                //then maybe the key exists in the sub objects or arrays
                //iterate over them
                Iterator it = obj.keySet().iterator();

                while (it.hasNext())
                {
                    String next = (String)it.next().toString();

                    if (obj.get(next) == null)
                    {
                        continue;
                    }

                    //if the value of the key is an object
                    //call the recursive function with the string value of the key
                    if (isObject(obj.get(next).toString()))
                    {
                        searchInner(obj.get(next).toString());
                    }
                    //if the value is an array
                    //send all the array elements as the string rep. of the object
                    else if (isArray(obj.get(next).toString()))
                    {
                        JSONArray arr = (JSONArray)parser.parse(obj.get(next).toString());

                        Iterator it2 = arr.iterator();
                        while (it2.hasNext())
                        {
                            JSONObject arrObj = (JSONObject)it2.next();
                            searchInner(arrObj.toString());
                        }
                    }
                    else
                    {
                        //means the value is not an object or an array
                        //dont iterate on values
                    }
                }
            }
        }
        catch(Exception ex)
        {
            if (!ex.getMessage().contains("java.lang.ClassCastException"))
            {
                //System.out.println(ex.getMessage());
                //System.out.println(this.value);
            }
            else
            {
                System.out.println(ex.toString());
                ex.printStackTrace();
            }
            return "";
        }
        finally
        {
            return retVal;
        }
    }

    public Boolean isArray(String gStr)
    {
        Boolean retVal = false;
        try
        {
            JSONParser parser = new JSONParser();
            JSONArray arr = (JSONArray)parser.parse(gStr);
            retVal = true;
        }
        catch(Exception ex)
        {
            retVal = false;
        }
        finally
        {
            return retVal;
        }
    }

    public Boolean isObject(String gStr)
    {
        Boolean retVal = false;
        try
        {
            JSONParser parser = new JSONParser();
            JSONObject obj = (JSONObject)parser.parse(gStr);
            retVal = true;
        }
        catch(Exception ex)
        {
            retVal = false;
        }
        finally
        {
            return retVal;
        }
    }

    public Boolean filter(String gFilter)
    {

        if (gFilter.equalsIgnoreCase("*"))
        {
            return true;
        }

        if (this.value.trim().toLowerCase().contains(gFilter.trim().toLowerCase()))
        {
            this.search = true;
            return true;
        }
        else
        {
            this.search = false;
            return false;
        }
    }

    public void regexFilter(String gFilter)
    {


    }

    public String getKey()
    {
        return this.key;
    }

    public String getValue()
    {
        return this.value;
    }
}