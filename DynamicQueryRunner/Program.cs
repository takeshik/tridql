// -*- mode: csharp; encoding: utf-8; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil; -*-
// vim:set ft=cs fenc=utf-8 ts=4 sw=4 sts=4 et:
// $Id$
/* DynamicQuery - Expression Tree Generating Language
 * Originally created by Microsoft Corporation.
 * Copyright © 2011 Takeshi KIRIYA (aka takeshik) <takeshik@users.sf.net>
 * 
 * This program is licensed under the Microsoft Public License (Ms-PL).
 */

using System;
using System.Linq.Dynamic;

internal class Program
{
    public static void Main(string[] args)
    {
        ((Action<String[]>)
            DynamicExpressions.ParseLambda(typeof(String[]), null, Console.In.ReadToEnd())
                .Compile()
        )(args);
    }
}
