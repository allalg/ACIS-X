# ICCSDI 2026 Conference LaTeX Manuscript Template

This package is intended exclusively for manuscript submissions to the **International Conference on Computational Science and Data Intelligence (ICCSDI 2026)**.

## Conference information

- **Conference:** International Conference on Computational Science and Data Intelligence (ICCSDI 2026)
- **Dates:** 11–12 December 2026
- **Venue:** NMIMS, Mumbai, India
- **Mode:** Hybrid

The package is for the ICCSDI 2026 conference paper only. Any separate invitation for an extended manuscript or another publication pathway will be communicated independently to selected authors.

## Main files

- `ICCSDI2026_Conference_Manuscript_Template.tex` — main editable conference manuscript
- `sn-article.tex` — equivalent default source file for compilation
- `ICCSDI2026_Conference_Manuscript_Template.pdf` — compiled preview
- `ICCSDI2026-references.bib` — sample bibliography database
- `sn-jnl.cls` — required LaTeX class file; do not edit
- `sn-mathphys-num.bst` and `bst/` — required bibliography-style files

## Preparing a manuscript

1. Open `ICCSDI2026_Conference_Manuscript_Template.tex` or `sn-article.tex`.
2. Replace the sample title, author names, affiliations, abstract, keywords, and instructional text.
3. Add figures to the project folder and insert them with `\includegraphics`.
4. Add references to `ICCSDI2026-references.bib`, or replace it with the authors’ bibliography database.
5. Do not modify the supplied class or bibliography-style files.
6. Follow the current ICCSDI 2026 scope, author guidelines, page limits, anonymity requirements, and submission instructions.

## Compilation

Compile with PDFLaTeX and BibTeX:

```text
pdflatex ICCSDI2026_Conference_Manuscript_Template.tex
bibtex ICCSDI2026_Conference_Manuscript_Template
pdflatex ICCSDI2026_Conference_Manuscript_Template.tex
pdflatex ICCSDI2026_Conference_Manuscript_Template.tex
```

The project can also be uploaded to Overleaf. Keep the `.tex`, `.cls`, selected `.bst`, `.bib`, and figure files together.

## Author checklist

- Remove all instructional text and placeholders.
- Define every abbreviation and mathematical symbol.
- Cite every table and figure in the manuscript text.
- Verify that figures remain legible at publication size.
- Report datasets, preprocessing, baselines, parameter settings, hardware, software versions, and evaluation procedures sufficiently for reproducibility.
- Complete all applicable declarations, including funding, competing interests, ethics, consent, data availability, code availability, and author contributions.
- Check the generated PDF carefully before submission.
