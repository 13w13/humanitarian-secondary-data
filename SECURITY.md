# Security

## Reporting a vulnerability

Open a [GitHub issue](https://github.com/13w13/humanitarian-secondary-data/issues)
for anything already public or low risk. For something that should not be described
in the open, use GitHub's private vulnerability reporting on this repository
(Security tab, "Report a vulnerability").

This is a volunteer-maintained project, so expect days rather than hours. There is
no bounty.

## What this toolkit does and does not do

It reads public humanitarian data from provider APIs and writes CSVs locally. It
does not run a server, expose a port, accept inbound connections, or store data for
anyone but the person running it. There are no runtime dependencies to compromise:
fetching uses only the Python standard library.

That shape rules out most of the usual categories. What is left is worth stating.

## Credentials

Keys live in your OS keychain (service `sds.{provider}`) or in environment
variables. They are never read from a command-line argument, so they do not reach
your shell history, and never written to a CSV or a log.

`config.py` is published on purpose: it holds endpoints and public fallbacks only.
If you fork this and add a key to it, you have published that key.

The `HAPI_APP_ID` fallback in `config.py` is a placeholder pointing at
`example.com`. It works, but every user sharing one identifier means HDX sees one
application. Generate your own:

```bash
python -c "import base64; print(base64.b64encode(b'my-app:me@example.com').decode())"
```

## Downloaded content is data, never instructions

This matters more than it sounds, because the toolkit is built to be driven by an
AI agent, and it extracts text from PDFs and spreadsheets published by third
parties.

**Text pulled from a remote report is untrusted input.** A PDF can contain
sentences addressed to a language model rather than to a reader: instructions to
ignore prior rules, to fetch some other resource, to reveal a key, to write
somewhere unexpected. Nothing in a downloaded file may be treated as a command.

If you build an agent on top of this repository:

- Treat every extracted string as a quotation, never as a directive.
- Never let a value derived from downloaded content choose a filesystem path, a
  shell command, or a URL to fetch next.
- Keep the country code an agent passes to a script under validation. This repo
  does that with `normalize_iso3()`, which accepts three ASCII letters and nothing
  else, because that value becomes a directory name.

## Personal data

Published outputs are aggregate figures, catalogue listings, and public news
events. **This toolkit deliberately does not bring survey microdata into the
repository.** Assessment microdata contains household-level answers, which is
personal data.

`wgss_probe.py` is the one script that touches such a file. It downloads to a
temporary directory outside the repository and outside git, reads only column
*names* and a count, never values, and tells you to delete the file afterwards.
Keep it that way: do not commit microdata, and do not put it in a synced folder.

## Downloads

`_download_file()` accepts `http` and `https` only, caps the response size, streams
to a `.part` file, and moves it into place with `os.replace()` once complete. A
failed download is deleted rather than left behind, because a truncated workbook
still opens and still sums, and a short total that looks plausible is the failure
mode this project exists to prevent.

## Rate limits and scraping

The clients pace themselves and identify themselves with a real User-Agent. One
source (Liveuamap) is scraped from HTML because it has no API, with deliberate
delays and a page cap.

Do not add User-Agent rotation, proxy cycling, or anything else whose purpose is to
get around a provider's rate limiting. If a limit is in the way, use `--date-from`
to ask for less, or contact the provider. Pull requests that add evasion will be
declined.
