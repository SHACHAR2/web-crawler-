using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.Text.RegularExpressions;
using System.Windows.Forms;

namespace Engine
{
    class Controller
    {
        Dictionary<string, string> cache = new Dictionary<string, string>();
        SortedDictionary<string, string[]> theDictionary = new SortedDictionary<string, string[]>();//term to (total tf, df, postingFileName, position in posting file)
        Indexer indexer = new Indexer();
        Dictionary<string, Document> DocDictionary = new Dictionary<string, Document>();
        /// <summary>
        /// run the engine, control all the classes
        /// </summary>
        /// <param name="path"></param>
        public void Engine(string path, string finalPath, bool stem)
        {
            Stemmer stemmer = new Stemmer();
            ReadFile rf = new ReadFile(path);
            Parser parser = new Parser(rf.ReadStopWords(path + "\\stop_words.txt"));
            int filesAmount = rf.FilesAmount();
            Document currentDoc = null;
            string tempPath1 = @"./temp Posting Files1";
            string tempPath2 = @"./temp Posting Files2";
            Directory.CreateDirectory(tempPath1);
            Directory.CreateDirectory(tempPath2);
            Directory.CreateDirectory(finalPath);
            string[] filesInTmp1 = Directory.GetFiles(tempPath1, "*.*", SearchOption.AllDirectories);
            for (int i = 0; i < filesInTmp1.Length; i++)
            {
                File.Delete(filesInTmp1[i]);
            }
            string[] filesInTmp2 = Directory.GetFiles(tempPath2, "*.*", SearchOption.AllDirectories);
            for (int i = 0; i < filesInTmp2.Length; i++)
            {
                File.Delete(filesInTmp2[i]);
            }
            DirectoryInfo di = new DirectoryInfo(path);
            long size = di.EnumerateFiles("*", SearchOption.AllDirectories).Sum(fi => fi.Length);
            long avgFilesSize = size / filesAmount;
            long tenPrecent = (size * 9) / 100;
            long numFiles = tenPrecent / avgFilesSize;
            int count = 0;
            //numFiles = 20;           
            for (int i = 0; i < filesAmount; i++)//going through the files in the dictionery and send each to the parser 
            {
                Match matchTEXT = rf.Seperate(i);// get a sperated files from red file
                while (matchTEXT.Success)
                {
                    Term[] terms = parser.Parse(matchTEXT.Groups[1].Value).Values.ToArray();
                    int max = -1;
                    if (stem)
                    {
                        for (int j = 0; j < terms.Length; j++)
                            terms[j].SetName(stemmer.stemTerm(terms[j].GetName()));
                    }
                    indexer.PrepareToPosting(terms, currentDoc = parser.GetDoc());
                    for (int j = 0; j < terms.Length; j++)
                    {
                        int currentTF = terms[j].GetTF(currentDoc);
                        if (currentTF > max)
                        {
                            max = currentTF;
                        }
                    }
                    currentDoc.SetMaxTF(max);
                    currentDoc.SetLength(terms.Length);
                    DocDictionary.Add(currentDoc.GetName(), currentDoc);
                    matchTEXT = matchTEXT.NextMatch();
                }
                count++;
                if (count == numFiles)
                {
                    Console.WriteLine("create posting");
                    indexer.CreateTempPostingFile(tempPath1);
                    count = 0;
                }
            }//for
            if (count > 0)// if we finished the for and there are still terms in the hash
            {
                indexer.CreateTempPostingFile(tempPath1);

            }
            int temporarlyPostingFolder1 = Directory.GetFiles(tempPath1, "*.*", SearchOption.AllDirectories).Length;
            int temporarlyPostingFolder2 = Directory.GetFiles(tempPath2, "*.*", SearchOption.AllDirectories).Length;
            //continue until there is only two files
            while (!(temporarlyPostingFolder1 == 2 && temporarlyPostingFolder2 == 0) || !(temporarlyPostingFolder1 == 0 && temporarlyPostingFolder2 == 2))
            {
                indexer.SetPostingNumber(0);
                Merge(tempPath1, tempPath2);
                temporarlyPostingFolder1 = Directory.GetFiles(tempPath1, "*.*", SearchOption.AllDirectories).Length;
                temporarlyPostingFolder2 = Directory.GetFiles(tempPath2, "*.*", SearchOption.AllDirectories).Length;
                if (temporarlyPostingFolder1 == 0 && temporarlyPostingFolder2 == 2)
                {
                    string[] temporarlyPostingFolder = Directory.GetFiles(tempPath2, "*.*", SearchOption.AllDirectories);
                    indexer.FinalMerge(temporarlyPostingFolder[0], temporarlyPostingFolder[1], finalPath, stem);
                    File.Delete(temporarlyPostingFolder[0]);
                    File.Delete(temporarlyPostingFolder[1]);
                    break;
                }
                indexer.SetPostingNumber(0);
                Merge(tempPath2, tempPath1);
                temporarlyPostingFolder1 = Directory.GetFiles(tempPath1, "*.*", SearchOption.AllDirectories).Length;
                temporarlyPostingFolder2 = Directory.GetFiles(tempPath2, "*.*", SearchOption.AllDirectories).Length;
                if (temporarlyPostingFolder1 == 2 && temporarlyPostingFolder2 == 0)
                {
                    string[] temporarlyPostingFolder = Directory.GetFiles(tempPath1, "*.*", SearchOption.AllDirectories);
                    indexer.FinalMerge(temporarlyPostingFolder[0], temporarlyPostingFolder[1], finalPath, stem);
                    File.Delete(temporarlyPostingFolder[0]);
                    File.Delete(temporarlyPostingFolder[1]);
                    break;
                }
            }
            theDictionary = indexer.GetFinalDic();
            test();
            test1();

            //cach
            List<string> tempTermList = theDictionary.Keys.ToList();
            List<int> totalTF = new List<int>();
            for (int i = 0; i < theDictionary.Values.Count; i++)
            {
                totalTF.Add(Int32.Parse(theDictionary[tempTermList[i]][0]));
            }
            totalTF.Sort((a, b) => -1 * a.CompareTo(b)); //descending sort
            HashSet<string> maxTF = new HashSet<string>();
            for (int i = 0; i < 10000; i++)
            {
                maxTF.Add(totalTF[i].ToString());
            }
            string s = totalTF[9999].ToString();
            int counter9999 = 0;
            string s1 = totalTF[9998].ToString();
            totalTF.Clear();
            for (int i = 0; i < theDictionary.Count; i++)
            {
                if (maxTF.Contains(theDictionary[tempTermList[i]][0]))
                {
                    if (theDictionary[tempTermList[i]][0].Equals(s))
                    {
                        counter9999++;
                        if (counter9999 > 6)
                        {
                            continue;
                        }
                    }
                    string pathtToPosting = Path.Combine(finalPath, theDictionary[tempTermList[i]][2]);
                    FileStream file = new FileStream(pathtToPosting, FileMode.Open, FileAccess.Read);
                    file.Seek(Int64.Parse(theDictionary[tempTermList[i]][3]), SeekOrigin.Begin);
                    BufferedStream bs = new BufferedStream(file);
                    StreamReader sr = new StreamReader(bs);
                    cache.Add(tempTermList[i], sr.ReadLine());
                }
            }
            tempTermList.Clear();
            Save(finalPath, stem);
        }//engine
         /// <summary>
         /// take every two files from source dictionery merge and save  the new file in the dest dictionery;
         /// </summary>
         /// <param name="source"></param>
         /// <param name="dest"></param>
        public void Merge(string source, string dest)
        {
            string[] temporarlyPostingFolder = Directory.GetFiles(source, "*.*", SearchOption.AllDirectories);
            int index = 0;//if even number of files
                          // if there id odd number of files in the source it move one file to the dest folder
            if (temporarlyPostingFolder.Length % 2 != 0)
            {
                string fileName = Path.GetFileName(temporarlyPostingFolder[0]);//take a file
                string destFile = Path.Combine(dest, fileName);//find new path to destination file
                File.Copy(temporarlyPostingFolder[0], destFile, true);//copy the file to the new path
                File.Delete(temporarlyPostingFolder[0]);//delete the file from the old path
                index = 1;
                indexer.SetPostingNumber(1);
            }
            for (int i = index; i < temporarlyPostingFolder.Length; i = i + 2)
            {
                indexer.Merge(temporarlyPostingFolder[i], temporarlyPostingFolder[i + 1], dest);//merge two file to destination direcory
                File.Delete(temporarlyPostingFolder[i]);
                File.Delete(temporarlyPostingFolder[i + 1]);
            }
        }
        /// <summary>
        /// delete the files in path1 and path2 and clear the dictionary and the cache
        /// </summary>
        /// <param name="path1"></param>
        /// <param name="path2"></param>
        public void Delete(string path1, string path2)
        {
            if (!path1.Equals(""))
            {
                string[] delete1 = Directory.GetFiles(path1, ".", SearchOption.AllDirectories);
                for (int i = 0; i < delete1.Length; i++)
                    File.Delete(delete1[i]);
            }
            if (!path2.Equals(""))
            {
                string[] delete2 = Directory.GetFiles(path2, ".", SearchOption.AllDirectories);
                for (int i = 0; i < delete2.Length; i++)
                    File.Delete(delete2[i]);
            }
            theDictionary.Clear();
            cache.Clear();
        }

        /// <summary>
        /// write the dictionary and the cache to files
        /// </summary>
        /// <param name="path"></param>
        public void Save(string path, bool stem)
        {
            List<string> tempTermList = theDictionary.Keys.ToList();
            List<string> tempTermList1 = cache.Keys.ToList();
            string pathDictionery = "";
            string pathCache = "";
            if(stem)
            {
                pathDictionery = path + "\\Poodel_DictionerySTM";
                pathCache = path + "\\Poodle_CacheSTM";
            }
            else
            {
                pathDictionery = path + "\\Poodel_Dictionery";
                pathCache = path + "\\Poodle_Cache";
            }
            StreamWriter saveDictionery = new StreamWriter(pathDictionery);
            StreamWriter saveCache = new StreamWriter(pathCache);
            for (int i = 0; i < theDictionary.Count; i++)
            {
                saveDictionery.WriteLine(tempTermList[i] + "total tf:" + theDictionary[tempTermList[i]][0] + ":" + theDictionary[tempTermList[i]][1] + ":" + theDictionary[tempTermList[i]][2] + ":" + theDictionary[tempTermList[i]][3]);
            }
            for (int i = 0; i < cache.Count; i++)
            {
                saveCache.WriteLine(tempTermList1[i] + "\\" + cache[tempTermList1[i]]);
            }
            saveDictionery.Close();
            saveCache.Close();
        }

        /// <summary>
        /// compute the size of the index in bytes
        /// </summary>
        /// <param name="finalPath"></param>
        /// <returns></returns>
        public long SizeOfIndex(string finalPath)
        {
            long l = 0;
            string[] final = Directory.GetFiles(finalPath, "*.*", SearchOption.AllDirectories);
            for(int i = 0; i < final.Length; i++)
            {
                FileInfo fi = new FileInfo(final[0]);
                l += fi.Length;
            }
            return l;
        }

        /// <summary>
        /// get path and load from there the dictionary and cache
        /// </summary>
        /// <param name="path"></param>
        public void Load(string path, bool stem)
        {
            string path1;
            if(stem)
            {
                path1 = path + "\\Poodel_DictionarySTM";
            }
            else
            {
                path1 = path + "\\Poodel_Dictionary";
            }
            StreamReader file1 = new StreamReader(path1);
            cache = new Dictionary<string, string>();
            theDictionary = new SortedDictionary<string, string[]>();
            while (!file1.EndOfStream)
            {                
                string line = file1.ReadLine();
                StringBuilder sb = new StringBuilder();
                int index = line.IndexOf("total tf:");
                for (int i = 0; i < index - 1; i++)
                {
                    sb.Append(line[i]);
                }
                string name = sb.ToString();
                sb.Clear();
                String[] detail = new string[4];
                int count = 0;
                for (int i = line.IndexOf("total tf:") + 9; i < line.Length; i++)
                {
                    if (line[i].Equals(":"))
                    {
                        detail[count] = sb.ToString();
                        count++;
                        sb.Clear();
                    }
                    else
                        sb.Append(line[i]);
                }
                theDictionary.Add(name, detail);
            }
            string path2 = path + "\\Poodle_Cache";
            StreamReader file2 = new StreamReader(path2);
            while (!file2.EndOfStream)
            {
                string line = file2.ReadLine();
                string[] split = line.Split('\\');
                cache.Add(split[0], split[1]);
            }
            file1.Close();
        }

        public int GetNumOfDocs()
        {
            return DocDictionary.Count();
        }
        public void test()
        {
            List<KeyValuePair<string, string[]>> a = theDictionary.ToList();
            a = a.OrderByDescending(av => av.Value[0]).ToList();
            StreamWriter st = new StreamWriter("file");
            a = a.OrderByDescending(asv => int.Parse(asv.Value[0])).ToList();

            foreach (KeyValuePair<string, string[]> item in a)
            {
                st.Write(item.Value[0] + " ");
            }
            st.Write("\n");
            int index = 1;
            foreach (KeyValuePair<string, string[]> item in a)
            {
                st.Write(index + " ");
                index++;
            }
            a = a.OrderBy(aav => int.Parse(aav.Value[0])).ToList();
            st.Close();
        }
        public void test1()
        {
            List<KeyValuePair<string, string[]>> a = theDictionary.ToList();
            a = a.OrderByDescending(aav =>int.Parse( aav.Value[1])).ToList();

            int b = 4;


            a = a.OrderBy(adv => int.Parse(adv.Value[1])).ToList();
        }

        public string GetDicPath(bool stem)
        {
            if (stem)
            {
                return "\\Poodel_DictionerySTM";
            }
            else
                return "\\Poodel_Dictionery";
        }
        public string GetCachePath(bool stem)
        {
            if (stem)
            {
                return "\\Poodel_CacheSTM";
            }
            else
                return "\\Poodel_Cache";
        }
    }
}



