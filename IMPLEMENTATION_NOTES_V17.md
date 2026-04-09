# V17 Shipment-Centric Simplification

What was implemented:
- Shipment-first workflow kept as the master structure
- New Customer Deals under each shipment
- New deal detail page with one-click document pack generation
- Excel generation from one XLSM template containing Invoice + Packing List
- CAD or Bill of Exchange generation from DOCX templates
- PDF conversion through LibreOffice headless when available
- ZIP export for the generated document pack
- Simpler shipment page and shipment detail page
- Customer Deals index page
- Search updated to include deals
- Navigation updated to surface Customer Deals more clearly

Main new routes:
- /export/deals
- /shipment/{id}/deal/add
- /deal/{id}
- /deal/{id}/generate

Main new data tables:
- shipment_deals
- deal_documents
