# Analytical Report: Strategic EV Infrastructure Roadmap 2027
**Team Colomany | IE-Iberdrola Datathon 2026**

## 1. Executive Summary
This report details the methodology and strategic deployment plan for Spain's interurban electric vehicle (EV) charging network for the year 2027. By integrating multi-modal data encompassing traffic flows, vehicle registration forecasts, and electrical grid capacity, we propose a high-power charging network that ensures national connectivity while proactively managing infrastructure bottlenecks. Our model identifies the optimal locations for 150 kW ultra-fast chargers, prioritizing the use of existing infrastructure, such as current charging hubs and gas stations, to accelerate the transition to sustainable mobility in a cost-effective manner.

---

## 2. Methodology & Analytical Pipeline

Our analytical approach is structured sequentially, beginning with a macroscopic view of future demand before narrowing down to highly localized spatial optimization. 

To ensure our proposed network is adequately scaled for the near future, we first developed a time-series forecasting model using Auto-ARIMA and SARIMAX architectures trained on monthly vehicle registration microdata from the Directorate-General for Traffic (DGT) spanning 2015 to 2025. This forecasting phase projects that electric vehicles will account for approximately 13.9% of the total vehicle fleet by December 2027. This penetration ratio serves as the vital demand multiplier that scales current traffic volumes to future requirements within our spatial model.

With demand quantified at a macro level, we established a spatial foundation by discretizing the Spanish interurban road network. The roads were sampled every 200 meters to create a high-resolution canvas of potential sites. We then mapped the Average Annual Daily Traffic (AADT) onto these points using a 50-meter spatial buffer, employing a longitudinal persistence filter to ensure that traffic data was accurately assigned to continuous road segments rather than noisy intersections or overpasses. Existing high-power chargers and gas stations were integrated directly into this network to anchor the analysis around viable, real-world locations.

Finally, this enriched spatial foundation fed into our core optimization engine, which utilizes Mixed-Integer Linear Programming (MILP). The MILP solver is designed to navigate the facility location problem by minimizing the total number of newly constructed greenfield sites and total chargers, while actively penalizing placements that would require expensive upgrades to the electrical grid. 

---

## 3. Assumptions & Technical Parameters

To ensure the model accurately reflects physical constraints and regulatory mandates, several critical technical assumptions were enforced throughout the analysis. 

Chief among these is the geographic coverage mandate. We implemented a strict 30-kilometer coverage radius for every interurban road segment, ensuring that a driver is never more than 60 kilometers away from a charging hub, which directly satisfies the European Union's AFIR regulations. To service these locations, the model assumes a standard power output of 150 kW per charger, operating with a daily throughput of 24 sessions per unit. This throughput assumes an average one-hour turnover time for a standard 20% to 80% ultra-fast charge.

Regarding infrastructure limitations, the model strictly caps the maximum capacity of any single site at 30 chargers, reflecting realistic physical space constraints at highway rest stops. Furthermore, we assume that a charging station must be within a 10-kilometer radius of an electrical substation to be considered viable for a medium-voltage connection. Finally, when translating raw traffic data into charging demand, we apply a 38% multiplier, representing the estimated proportion of medium to long-distance interurban travelers who realistically require en-route charging rather than charging at their origin or destination.

---

## 4. Grid Status Classification & Justification

A defining feature of our strategic roadmap is its inherent grid-awareness. We evaluate the feasibility of every proposed location by calculating a Capacity-to-Demand ratio, comparing the available capacity at the nearest electrical substation to the estimated load of the proposed charging hub. Based on this ratio, sites are classified into three distinct categories.

Locations are deemed "Sufficient" (Green) if the local substation has enough available capacity to comfortably support the new load. Conversely, sites are classified as "Congested" (Red) if the projected demand significantly exceeds the available capacity by more than 20%, or if the nearest substation is entirely inaccessible (greater than 10 kilometers away). These congested locations represent areas where high capital expenditure will be necessary.

Crucially, we introduced a "Moderate" (Yellow) classification for sites where demand slightly exceeds the stated capacity, but remains within a 20% tolerance margin. This 20% buffer is a deliberate strategic assumption intended to account for peak demand variance. Electrical grid planning must accommodate highly concentrated, spiky loads—such as holiday travel weekends—meaning that a substation operating near its theoretical limit may require strategic oversight, but not necessarily a complete structural overhaul. 

---

## 5. Strategic Proposals

Based on our analysis, we advocate for a phased deployment strategy that balances immediate connectivity needs with long-term grid sustainability. 

The initial rollout phase should aggressively target existing gas stations that reside within "Sufficient" grid zones. By leveraging locations that already possess commercial permits and strong grid access, Iberdrola can rapidly deploy infrastructure with minimal lead times. Following this, a secondary phase should focus on establishing new greenfield sites strictly to eliminate "dead zones" where network coverage falls below the AFIR mandate. 

Finally, for the inevitable "Friction Points"—locations where high mobility demand collides with Moderate or Congested grid status—we propose a strategic shift. Rather than immediately committing to expensive and time-consuming substation reinforcements, Iberdrola should deploy localized Battery Energy Storage Systems (BESS) at these specific sites. These storage systems can draw power during off-peak hours and discharge during periods of high demand, effectively bridging the capacity gap and providing a flexible, cost-effective alternative to traditional grid upgrades.

---

## 6. Limitations & Risk Assessment

While our model provides a robust, data-driven foundation for infrastructure planning, it is subject to several inherent limitations. First, the grid capacity data provided by distributors represents a static snapshot; real-time available capacity may fluctuate as new industrial access permits are granted. Second, our demand forecasting inherently assumes a stable macroeconomic environment and relies on vehicle registration trends continuing as projected. Should battery technology leap forward, enabling ubiquitous 15-minute charging times, the required density of chargers could decrease significantly, altering the optimal deployment map. Finally, our 200-meter geospatial discretization, while excellent for macro-level routing, may occasionally overlook micro-scale land availability nuances that human engineers would catch during a final site survey.

---

## 7. Data Sources & Citations

The integrity of this analysis relies on the following official public datasets:

1. **Ministry of Transport and Sustainable Mobility (MITMA):** Interurban road network topology sourced from the Hermes Portal (Red de Carreteras del Estado), and mobility tracking data sourced from the Movilidad Big Data open portal.
2. **Directorate-General for Traffic (DGT):** Historical vehicle registration microdata, alongside the baseline inventory of existing electric vehicle charging points from the National Access Point (NAP).
3. **Ministry for the Ecological Transition (MITECO):** Geoportal registry of traditional gas stations.
4. **Electrical Distributors:** Node-level and substation-level consumption capacity maps provided by i-DE (Iberdrola Group), e-distribución (Endesa), and Viesgo Distribución.
