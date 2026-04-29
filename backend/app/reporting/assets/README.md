## Reporting Brand Assets

Put PDF branding assets in this folder structure:

- `assets/fonts/BrandSans-Regular.ttf`
- `assets/logos/company_logo.png`

The PDF exporter auto-loads these defaults if they exist.

### Notes

- Font file currently referenced by code: `BrandSans-Regular.ttf`
- Logo file currently referenced by code: `company_logo.png`
- Relative asset paths are resolved using WeasyPrint `base_url` set to the `reporting` directory.
