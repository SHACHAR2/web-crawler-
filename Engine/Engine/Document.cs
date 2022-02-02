using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Collections;

namespace Engine
{
    class Document
    {
        private string name;
        int max_tf;
        int length;
        string date;
        public Document(string name)
        {
            this.name = name;
            max_tf = 0;
            length = 0;
        }
        public void SetLength(int length)
        {
            this.length = length;
        }
        public void SetDate(string date)
        {
            this.date = date;
        }

        /// <summary>
        /// compare to terms according to there name 
        /// </summary>
        /// <param name="obj"></param>
        /// <returns>true- if two terms have the same name 
        /// false- if two terms dosent have the same name</returns>
        public override bool Equals(object obj)
        {
            return name.Equals(((Document)obj).name);
        }

        public String GetName()
        {
            return name;
        }

        public void SetMaxTF(int maxTF)
        {
            this.max_tf = maxTF;
        }
    }
}