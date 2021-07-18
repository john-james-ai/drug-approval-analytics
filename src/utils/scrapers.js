// Project  : Drug Approval Analytics
// Version  : 0.1.0
// File     : \src\utils\scrapers.js
// Language : Python 3.9.5
// -----------------------------------------------------------------------------
// Author   : John James
// Company  : nov8.ai
// Email    : john.james@nov8.ai
// URL      : https://github.com/john-james-sf/drug-approval-analytics
// -----------------------------------------------------------------------------
// Created  : Sunday, July 18th 2021, 7:33:27 am
// Modified : Sunday, July 18th 2021, 7:38:11 am
// Modifier : John James (john.james@nov8.ai)
// -----------------------------------------------------------------------------
// License  : BSD 3-clause "New" or "Revised" License
// Copyright: (c) 2021 nov8.ai

const puppeteer = require('puppeteer');

const express = require('express');
const app = express();
const port = 3000;

app.get('/', async (req, res) => {
    const {url} = req.query;
    if(!url) {
        res.status(400).send("Bad request: 'url' param is missing!");
        return;
    }

    try {
        const html = await getPageHTML(url);

        res.status(200).send(html);
    } catch (error) {
        res.status(500).send(error);
    }
});

const getPageHTML = async (pageUrl) => {
    const browser = await puppeteer.launch();
  
    const page = await browser.newPage();
  
    await page.goto(pageUrl);
  
    const pageHTML = await page.evaluate('new XMLSerializer().serializeToString(document.doctype) + document.documentElement.outerHTML');
  
    await browser.close();
  
    return pageHTML;
}

app.listen(port, () => console.log(`Example app listening on port ${port}!`))