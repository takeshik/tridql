// -*- mode: csharp; encoding: utf-8; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil; -*-
// vim:set ft=cs fenc=utf-8 ts=4 sw=4 sts=4 et:
// $Id$
/* TriDQL - Expression Tree Generating Language
 * Copyright © 2011 Takeshi KIRIYA (aka takeshik) <takeshik@users.sf.net>
 * TriDQL is fork of DynamicQuery, originally created by Microsoft Corporation.
 * 
 * This library is licensed under the Microsoft Public License (Ms-PL).
 */

using System;

namespace XSpect.TriDQL.Runner
{
    internal class Program
    {
        public static void Main(String[] args)
        {
            try
            {
                Action<String[]> code = ((Action<String[]>)
                    TriDQL.ParseLambda(typeof(String[]), null, Console.In.ReadToEnd())
                        .Compile()
                );
                try
                {
                    code(args);
                }
                catch(Exception ex)
                {
                    Console.ForegroundColor = ConsoleColor.Magenta;
                    Console.WriteLine("EXECUTE FAILED.");
                    Console.Error.WriteLine(ex.Message);
                    Console.Error.WriteLine(ex.StackTrace);
                    Console.ResetColor();
                }

            }
            catch(Exception ex)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine("COMPILE FAILED.");
                Console.Error.WriteLine(ex.Message);
                Console.Error.WriteLine(ex.StackTrace);
                Console.ResetColor();
            }
        }
    }
}