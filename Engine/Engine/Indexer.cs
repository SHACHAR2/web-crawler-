using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.Collections;



namespace Engine
{
    class Indexer
    {
        SortedDictionary<string, string[]> finalDic = new SortedDictionary<string, string[]>();
        Dictionary<string, StringBuilder> termsToPosting = new Dictionary<string, StringBuilder>();
        int postingNumber;//inner counter to the posting files during each merge from source file to destination file
        int tempPostingFilesCounter;//counter to the posting files during the creation of the first posting files
        public Indexer()
        {
            postingNumber = 0;
            tempPostingFilesCounter = 0;
        }
        /// <summary>
        /// create a string of terms that will be written to a temporarly posting file when there are enogth terms( enoghth= 10% of the courpes)
        /// </summary>
        /// <param name="terms"></param>
        /// <param name="currentDoc"></param>
        public void PrepareToPosting(Term[] terms, Document currentDoc)
        {
            for (int i = 0; i < terms.Length; i++)
            {
                if (termsToPosting.ContainsKey(terms[i].GetName()))
                {
                    termsToPosting[terms[i].GetName()].Append(terms[i].StringToPosting(currentDoc));
                }
                else
                {
                    termsToPosting.Add(terms[i].GetName(), new StringBuilder(terms[i].StringToPosting(currentDoc)));
                }
            }
        }
        /// <summary>
        /// sort the list of terms, merge the difrrent parts of the term and write them on the temporarly posting file
        /// </summary>
        /// <param name="path"> the where to write the temporarly posting file</param>
        public void CreateTempPostingFile(string path)
        {
            List<string> tempTermList = termsToPosting.Keys.ToList();
            StringBuilder str = new StringBuilder("");
            tempTermList.Sort();
            for (int i = 0; i < tempTermList.Count; i++)
            {
                str.Append(tempTermList[i]).Append("|" + termsToPosting[tempTermList[i]].ToString() + "\n");
            }
            termsToPosting.Clear();
            string newPath = path + "\\TempPostingFileNumber_" + tempPostingFilesCounter;
            File.WriteAllText(newPath, str.ToString());
            tempPostingFilesCounter++;
        }

        /// <summary>
        /// merge two files, from pathFile1 and pathFile2, to one file in pathMerge
        /// </summary>
        /// <param name="pathFile1"></param>  path of file1
        /// <param name="pathFile2"></param> path of file2
        /// <param name="pathMerge"></param> path of destination merged file
        public void Merge(string pathFile1, string pathFile2, string pathMerge)
        {
            string line1 = null;
            string line2 = null;
            SortedDictionary<string, StringBuilder> tempDic = new SortedDictionary<string, StringBuilder>();//holds 3 of each posting file (1 or 2)
            StreamReader file1 = new StreamReader(pathFile1);
            StreamReader file2 = new StreamReader(pathFile2);
            string pathPosting = pathMerge + "/posting" + postingNumber;
            StreamWriter posting = new StreamWriter(pathPosting);
            postingNumber++;
            StringBuilder sb = new StringBuilder();
            string last1 = "";
            string last2 = "";
            bool shirshur = false;
            if ((!file1.EndOfStream))
            {
                if ((line1 = file1.ReadLine()) != null)
                {
                    sb.Clear();
                    int index = 0;
                    while (!line1[index].Equals('\0'))
                    {
                        if (!line1[index].Equals('|'))
                        {
                            sb.Append(line1[index]);
                            index++;
                        }
                        else
                            break;
                    }
                    if (!tempDic.ContainsKey(sb.ToString()))
                    {
                        tempDic.Add(sb.ToString(), new StringBuilder(line1));
                    }
                    else
                    {
                        tempDic[sb.ToString()].Append(line1.Replace(sb.ToString() + "|", ""));
                    }
                    last1 = sb.ToString();
                }
            }
            if ((!file2.EndOfStream))
            {
                if ((line2 = file2.ReadLine()) != null)
                {
                    sb.Clear();
                    int index = 0;
                    while (!line2[index].Equals('\0'))
                    {
                        if (!line2[index].Equals('|'))
                        {
                            sb.Append(line2[index]);
                            index++;
                        }
                        else
                            break;
                    }
                    if (!tempDic.ContainsKey(sb.ToString()))
                    {
                        tempDic.Add(sb.ToString(), new StringBuilder(line2));
                    }
                    else
                    {
                        tempDic[sb.ToString()].Append(line2.Replace(sb.ToString() + "|", ""));
                        shirshur = true;
                    }
                    last2 = sb.ToString();
                }
            }
            while ((!file1.EndOfStream) && (!file2.EndOfStream))// going throgh the files and stop when he got to the end of them 
            {
                posting.Write(tempDic.ElementAt(0).Value + "\n");
                posting.Flush();
                tempDic.Remove(tempDic.ElementAt(0).Key);
                if (!shirshur)
                {
                    if (String.Compare(last1, last2) < 0)
                    {
                        if ((!file1.EndOfStream))
                        {
                            if ((line1 = file1.ReadLine()) != null)
                            {
                                sb.Clear();
                                int index = 0;
                                while (!line1[index].Equals('\0'))
                                {
                                    if (!line1[index].Equals('|'))
                                    {
                                        sb.Append(line1[index]);
                                        index++;
                                    }
                                    else
                                        break;
                                }
                                if (!tempDic.ContainsKey(sb.ToString()))
                                {
                                    tempDic.Add(sb.ToString(), new StringBuilder(line1));
                                    shirshur = false;
                                }
                                else
                                {
                                    tempDic[sb.ToString()].Append(line1.Replace(sb.ToString() + "|", ""));
                                    shirshur = true;
                                }
                                last1 = sb.ToString();
                            }
                        }
                    }
                    else
                    {
                        if ((!file2.EndOfStream))
                        {
                            if ((line2 = file2.ReadLine()) != null)
                            {
                                sb.Clear();
                                int index = 0;
                                while (!line2[index].Equals('\0'))
                                {
                                    if (!line2[index].Equals('|'))
                                    {
                                        sb.Append(line2[index]);
                                        index++;
                                    }
                                    else
                                        break;
                                }
                                if (!tempDic.ContainsKey(sb.ToString()))
                                {
                                    tempDic.Add(sb.ToString(), new StringBuilder(line2));
                                    shirshur = false;
                                }
                                else
                                {
                                    tempDic[sb.ToString()].Append(line2.Replace(sb.ToString() + "|", ""));
                                    shirshur = true;
                                }
                                last2 = sb.ToString();
                            }
                        }
                    }
                }
                else
                {
                    if ((!file1.EndOfStream))
                    {
                        if ((line1 = file1.ReadLine()) != null)
                        {
                            sb.Clear();
                            int index = 0;
                            while (!line1[index].Equals('\0'))
                            {
                                if (!line1[index].Equals('|'))
                                {
                                    sb.Append(line1[index]);
                                    index++;
                                }
                                else
                                    break;
                            }
                            if (!tempDic.ContainsKey(sb.ToString()))
                            {
                                tempDic.Add(sb.ToString(), new StringBuilder(line1));
                                shirshur = true;
                            }
                            else
                            {
                                tempDic[sb.ToString()].Append(line1.Replace(sb.ToString() + "|", ""));
                                shirshur = false;
                            }
                            last1 = sb.ToString();
                        }
                    }
                    if ((!file2.EndOfStream))
                    {
                        if ((line2 = file2.ReadLine()) != null)
                        {
                            sb.Clear();
                            int index = 0;
                            while (!line2[index].Equals('\0'))
                            {
                                if (!line2[index].Equals('|'))
                                {
                                    sb.Append(line2[index]);
                                    index++;
                                }
                                else
                                    break;
                            }
                            if (!tempDic.ContainsKey(sb.ToString()))
                            {
                                tempDic.Add(sb.ToString(), new StringBuilder(line2));
                                shirshur = false;
                            }
                            else
                            {
                                tempDic[sb.ToString()].Append(line2.Replace(sb.ToString() + "|", ""));
                                shirshur = true;
                            }
                            last2 = sb.ToString();
                        }
                    }
                }
            }
            if (!file1.EndOfStream)
            {
                while (!file1.EndOfStream)
                {
                    if ((line1 = file1.ReadLine()) != null)
                    {
                        sb.Clear();
                        int index = 0;
                        while (!line1[index].Equals('\0'))
                        {
                            if (!line1[index].Equals('|'))
                            {
                                sb.Append(line1[index]);
                                index++;
                            }
                            else
                                break;
                        }
                        if (!tempDic.ContainsKey(sb.ToString()))
                        {
                            tempDic.Add(sb.ToString(), new StringBuilder(line1));
                        }
                        else
                        {
                            tempDic[sb.ToString()].Append(line1.Replace(sb.ToString() + "|", ""));
                        }
                        posting.Write(tempDic.ElementAt(0).Value + "\n");
                        posting.Flush();
                        tempDic.Remove(tempDic.ElementAt(0).Key);
                    }
                }
            }
            else if (!file2.EndOfStream)
            {
                while (!file2.EndOfStream)
                {
                    if ((line2 = file2.ReadLine()) != null)
                    {
                        sb.Clear();
                        int index = 0;
                        while (!line1[index].Equals('\0'))
                        {
                            if (!line1[index].Equals('|'))
                            {
                                sb.Append(line1[index]);
                                index++;
                            }
                            else
                                break;
                        }
                        if (!tempDic.ContainsKey(sb.ToString()))
                        {
                            tempDic.Add(sb.ToString(), new StringBuilder(line1));
                        }
                        else
                        {
                            tempDic[sb.ToString()].Append(line1.Replace(sb.ToString() + "|", ""));
                        }
                        posting.Write(tempDic.ElementAt(0).Value + "\n");
                        posting.Flush();
                        tempDic.Remove(tempDic.ElementAt(0).Key);
                    }
                }
            }
            while (tempDic.Count > 0)
            {
                posting.Write(tempDic.ElementAt(0).Value + "\n");
                posting.Flush();
                tempDic.Remove(tempDic.ElementAt(0).Key);
            }
            file1.Close();
            file2.Close();
            posting.Close();
        }

        public void SetPostingNumber(int num)
        {
            postingNumber = num;
        }

        /// <summary>
        /// marge the last two files and create the dictionery
        /// </summary>
        /// <param name="pathFile1"></param>
        /// <param name="pathFile2"></param>
        /// <param name="finalPath"></param>
        public void FinalMerge(string pathFile1, string pathFile2, string finalPath, bool stem)
        {
            StreamReader file1 = new StreamReader(pathFile1);
            StreamReader file2 = new StreamReader(pathFile2);
            if (!stem)
            {
                string pathPosting = finalPath + "/Numbers posting";//default
                StreamWriter final_posting = new StreamWriter(pathPosting);
                List<char> letters = new List<char> { 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z' };
                Posting(final_posting, file1, file2, '0', "Numbers Posting");
                final_posting.Close();
                for (int i = 0; i < 26; i++)
                {
                    pathPosting = finalPath + "/" + letters[i] + "posting";
                    final_posting = new StreamWriter(pathPosting);
                    Posting(final_posting, file1, file2, char.ToLower(letters[i]), letters[i] + "Posting");
                    final_posting.Close();
                }
                final_posting.Close();
            }
            else
            {
                string pathPosting = finalPath + "/Numbers postingSTM";//default
                StreamWriter final_posting = new StreamWriter(pathPosting);
                List<char> letters = new List<char> { 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z' };
                Posting(final_posting, file1, file2, '0', "Numbers PostingSTM");
                final_posting.Close();
                for (int i = 0; i < 26; i++)
                {
                    pathPosting = finalPath + "/" + letters[i] + "postingSTM";
                    final_posting = new StreamWriter(pathPosting);
                    Posting(final_posting, file1, file2, char.ToLower(letters[i]), letters[i] + "PostingSTM");
                    final_posting.Close();
                }
                final_posting.Close();
            }
            file1.Close();
            file2.Close();
        }
        /// <summary>
        /// merge between two files as long as the first letter of the words in the documents equals to topic 
        /// </summary>
        /// <param name="final_posting"></param>
        /// <param name="file1"></param>
        /// <param name="file2"></param>
        /// <param name="topic"></param>
        /// <param name="name"></param>
        private void Posting(StreamWriter final_posting, StreamReader file1, StreamReader file2, char topic, string name)
        {
            string line1;
            string line2;
            StringBuilder term1 = new StringBuilder();
            StringBuilder term2 = new StringBuilder();
            StringBuilder sb = new StringBuilder();
            if ((line1 = file1.ReadLine()) != null)
            {
                term1.Clear();
                int index = 0;
                while (!line1[index].Equals('\0'))
                {
                    if (!line1[index].Equals('|'))
                    {
                        term1.Append(line1[index]);
                        index++;
                    }
                    else
                        break;
                }
            }
            if ((line2 = file2.ReadLine()) != null)
            {
                term2.Clear();
                int index = 0;
                while (!line2[index].Equals('\0'))
                {
                    if (!line2[index].Equals('|'))
                    {
                        term2.Append(line2[index]);
                        index++;
                    }
                    else
                        break;
                }
            }
            StringBuilder lineToWrite = new StringBuilder();
            while (!(file1.EndOfStream) && !(file2.EndOfStream) && Condition(term1, topic) && Condition(term2, topic))
            {
                lineToWrite.Clear();
                if (term1.ToString().CompareTo(term2.ToString()) < 0)
                {
                    lineToWrite.Append(line1.Substring(line1.IndexOf("|") + 1));//replace??
                    sb = new StringBuilder(term1.ToString());
                    if ((line1 = file1.ReadLine()) != null)
                    {
                        term1.Clear();
                        int index = 0;
                        while (!line1[index].Equals('\0'))
                        {
                            if (!line1[index].Equals('|'))
                            {
                                term1.Append(line1[index]);
                                index++;
                            }
                            else
                                break;
                        }
                    }
                    while (sb.Equals(term1) && !(file1.EndOfStream))
                    {
                        lineToWrite.Append(line1.Substring(line1.IndexOf("|") + 1));
                        if ((line1 = file1.ReadLine()) != null)
                        {
                            term1.Clear();
                            int index = 0;
                            while (!line1[index].Equals('\0'))
                            {
                                if (!line1[index].Equals('|'))
                                {
                                    term1.Append(line1[index]);
                                    index++;
                                }
                                else
                                    break;
                            }
                        }
                    }
                }
                else if (term1.ToString().CompareTo(term2.ToString()) > 0)
                {

                    lineToWrite.Append(line2.Substring(line2.IndexOf("|") + 1));
                    sb = new StringBuilder(term2.ToString());
                    if ((line2 = file2.ReadLine()) != null)
                    {
                        term2.Clear();
                        int index = 0;
                        while (!line2[index].Equals('\0'))
                        {
                            if (!line2[index].Equals('|'))
                            {
                                term2.Append(line2[index]);
                                index++;
                            }
                            else
                                break;
                        }
                    }
                    while (sb.Equals(term2) && !(file2.EndOfStream))
                    {
                        lineToWrite.Append(line2.Substring(line2.IndexOf("|") + 1));
                        if ((line2 = file2.ReadLine()) != null)
                        {
                            term2.Clear();
                            int index = 0;
                            while (!line2[index].Equals('\0'))
                            {
                                if (!line2[index].Equals('|'))
                                {
                                    term2.Append(line2[index]);
                                    index++;
                                }
                                else
                                    break;
                            }
                        }
                    }
                }
                else// =0 
                {
                    sb = new StringBuilder(term1.ToString());
                    lineToWrite.Append(line2.Substring(line2.IndexOf("|") + 1));
                    lineToWrite.Append(line1.Substring(line1.IndexOf("|")));
                    if ((line1 = file1.ReadLine()) != null)
                    {
                        term1.Clear();
                        int index = 0;
                        while (!line1[index].Equals('\0'))
                        {
                            if (!line1[index].Equals('|'))
                            {
                                term1.Append(line1[index]);
                                index++;
                            }
                            else
                                break;
                        }
                    }
                    if ((line2 = file2.ReadLine()) != null)
                    {
                        term2.Clear();
                        int index = 0;
                        while (!line2[index].Equals('\0'))
                        {
                            if (!line2[index].Equals('|'))
                            {
                                term2.Append(line2[index]);
                                index++;
                            }
                            else
                                break;
                        }
                    }
                }

                int filesAmount = 0;// the amount of files the term apper in, df
                StringBuilder tf = new StringBuilder();
                int sum = 0;
                for (int i = 0; i < lineToWrite.Length; i++)
                {
                    if (lineToWrite[i].Equals('_'))
                    {

                        i++;
                        while (!lineToWrite[i].Equals('_'))
                        {

                            tf.Append(lineToWrite[i]);
                            i++;
                        }
                        sum += Int32.Parse(tf.ToString());
                        tf.Clear();
                        filesAmount++;
                    }
                }
                string[] termDetails = new string[4];// termDetails[0]= df, termDetails[1]= name of final postnig file,  termDetails[2]= position in the posting file
                termDetails[0] = sum.ToString();
                termDetails[1] = filesAmount.ToString();
                termDetails[2] = name;
                termDetails[3] = final_posting.BaseStream.Length.ToString();
                if (sum > 2)
                {
                    string temp = sb.ToString();
                    if (sb.ToString().Contains('�'))//לבדוק
                        continue;                   
                   if (!finalDic.ContainsKey(sb.ToString()))
                    {
                        final_posting.WriteLine(lineToWrite.ToString() + "/n");
                        finalDic.Add(sb.ToString(), termDetails);
                    }
                    else
                        Console.WriteLine(sb.ToString());
                 }
            }//while 
            if (!(file1.EndOfStream) && !(file2.EndOfStream))
            {
                while (Condition(term1, topic) && !Condition(term2, topic))
                {
                    int filesAmount = 0;// the amount of files the term apper in
                    StringBuilder tf = new StringBuilder(); ;
                    int sum = 0;                  
                    for (int i = 0; i < lineToWrite.Length; i++)
                    {
                        if (lineToWrite[i].Equals('_'))
                        {
                            i++;
                            while (!lineToWrite[i].Equals('_'))
                            {
                                tf.Append(lineToWrite[i]);
                                i++;
                            }
                            sum += Int32.Parse(tf.ToString());
                            tf.Clear();
                            filesAmount++;
                        }
                    }
                    string[] termDetails = new string[4];// termDetails[0]= df, termDetails[1]= name of final postnig file,  termDetails[2]= position in the posting file
                    termDetails[0] = sum.ToString();
                    termDetails[1] = filesAmount.ToString();
                    termDetails[2] = name;
                    termDetails[3] = final_posting.BaseStream.Length.ToString();
                    if (sum > 2)
                    {
                        string temp = term1.ToString();
                        if (term1.ToString().Contains('�'))
                            continue;                       
                        if (!finalDic.ContainsKey(term1.ToString()))
                        {
                            final_posting.WriteLine(lineToWrite.ToString() + "/n");
                            finalDic.Add(term1.ToString(), termDetails);
                        }
                        else
                            Console.WriteLine(sb.ToString());        
                    }
                    if ((line1 = file1.ReadLine()) != null)
                    {
                        term1.Clear();
                        int index = 0;
                        while (!line1[index].Equals('\0'))
                        {
                            if (!line1[index].Equals('|'))
                            {
                                term1.Append(line1[index]);
                                index++;
                            }
                            else
                                break;
                        }
                    }
                    else
                        break;
                }
                while (!Condition(term1, topic) && Condition(term2, topic))
                {
                    int filesAmount = 0;// the amount of files the term apper in
                    StringBuilder tf = new StringBuilder(); ;
                    int sum = 0;

                    for (int i = 0; i < lineToWrite.Length; i++)
                    {
                        if (lineToWrite[i].Equals('_'))
                        {

                            i++;
                            while (!lineToWrite[i].Equals('_'))
                            {

                                tf.Append(lineToWrite[i]);
                                i++;
                            }
                            sum += Int32.Parse(tf.ToString());
                            tf.Clear();
                            filesAmount++;
                        }
                    }
                    string[] termDetails = new string[4];// termDetails[0]= df, termDetails[1]= name of final postnig file,  termDetails[2]= position in the posting file
                    termDetails[0] = sum.ToString();
                    termDetails[1] = filesAmount.ToString();
                    termDetails[2] = name;
                    termDetails[3] = final_posting.BaseStream.Length.ToString();
                    if (sum > 2)
                    {
                        string temp = term2.ToString();
                        if (term2.ToString().Contains('�'))
                            continue;
                       
                        if (!finalDic.ContainsKey(term2.ToString()))
                        {
                            final_posting.WriteLine(lineToWrite.ToString() + "/n");
                            finalDic.Add(term2.ToString(), termDetails);
                        }
                        else
                            Console.WriteLine(sb.ToString());                    
                    }
                    
                    if ((line2 = file2.ReadLine()) != null)
                    {
                        term2.Clear();
                        int index = 0;
                        while (!line2[index].Equals('\0'))
                        {
                            if (!line2[index].Equals('|'))
                            {
                                term2.Append(line2[index]);
                                index++;
                            }
                            else
                                break;
                        }
                    }
                    else
                        break;
                }
            }
            else if (!(file1.EndOfStream))
            {
                while (!(file1.EndOfStream))
                {
                    int filesAmount = 0;// the amount of files the term apper in
                    StringBuilder tf = new StringBuilder(); ;
                    int sum = 0;

                    for (int i = 0; i < lineToWrite.Length; i++)
                    {
                        if (lineToWrite[i].Equals('_'))
                        {

                            i++;
                            while (!lineToWrite[i].Equals('_'))
                            {

                                tf.Append(lineToWrite[i]);
                                i++;
                            }
                            sum += Int32.Parse(tf.ToString());
                            tf.Clear();
                            filesAmount++;
                        }
                    }
                    string[] termDetails = new string[4];// termDetails[0]= df, termDetails[1]= name of final postnig file,  termDetails[2]= position in the posting file
                    termDetails[0] = sum.ToString();
                    termDetails[1] = filesAmount.ToString();
                    termDetails[2] = name;
                    termDetails[3] = final_posting.BaseStream.Length.ToString();
                    if (sum > 2)
                    {
                        string temp = term1.ToString();
                        if (term1.ToString().Contains('�'))
                            continue;
                        
                        if (!finalDic.ContainsKey(sb.ToString()))
                        {
                            final_posting.WriteLine(lineToWrite.ToString() + "/n");
                            finalDic.Add(term1.ToString(), termDetails);
                        }
                        else
                            Console.WriteLine(sb.ToString());
                        
                    }
                   
                    if ((line1 = file1.ReadLine()) != null)
                    {
                        term1.Clear();
                        int index = 0;
                        while (!line1[index].Equals('\0'))
                        {
                            if (!line1[index].Equals('|'))
                            {
                                term1.Append(line1[index]);
                                index++;
                            }
                            else
                                break;
                        }
                    }
                }
            }
            else
            {
                while (!file2.EndOfStream)
                {
                    int filesAmount = 0;// the amount of files the term apper in
                    StringBuilder tf = new StringBuilder(); ;
                    int sum = 0;

                    for (int i = 0; i < lineToWrite.Length; i++)
                    {
                        if (lineToWrite[i].Equals('_'))
                        {

                            i++;
                            while (!lineToWrite[i].Equals('_'))
                            {

                                tf.Append(lineToWrite[i]);
                                i++;
                            }
                            sum += Int32.Parse(tf.ToString());
                            tf.Clear();
                            filesAmount++;
                        }
                    }
                    string[] termDetails = new string[4];// termDetails[0]= df, termDetails[1]= name of final postnig file,  termDetails[2]= position in the posting file
                    termDetails[0] = sum.ToString();
                    termDetails[1] = filesAmount.ToString();
                    termDetails[2] = name;
                    termDetails[3] = final_posting.BaseStream.Length.ToString();
                    if (sum > 2)
                    {
                        string temp = term2.ToString();
                        if (term2.ToString().Contains('�'))
                            continue;               
                        if (!finalDic.ContainsKey(term2.ToString()))
                        {
                            final_posting.WriteLine(lineToWrite.ToString() + "/n");
                            finalDic.Add(term2.ToString(), termDetails);
                        }
                        else
                            Console.WriteLine(sb.ToString());                     
                    }                  
                    if ((line1 = file1.ReadLine()) != null)
                    {
                        term1.Clear();
                        int index = 0;
                        while (!line1[index].Equals('\0'))
                        {
                            if (!line1[index].Equals('|'))
                            {
                                term1.Append(line1[index]);
                                index++;
                            }
                            else
                                break;
                        }
                    }
                }
            }
        }
        /// <summary>
        /// check if the word belong to some topic, if the first lleter in the word belong to the topic
        /// </summary>
        /// <param name="term1"></param>
        /// <param name="topic"></param>
        /// <returns></returns>
        private bool Condition(StringBuilder term1, Char topic)
        {
            bool ans = false;
            string temp = term1.ToString();
            if (topic.Equals('0'))// number case 
            {
                ans = char.IsNumber(temp[0]);
            }
            else
            {
                temp = temp.ToLower();
                if (temp[0].Equals(topic))
                    ans = true;
            }
            return ans;
        }
        public SortedDictionary<string, string[]> GetFinalDic()
        {
            return finalDic;
        }
    }//class
}//name space