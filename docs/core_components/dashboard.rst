Dashboards
==========

Tshistory Dashboard is a comprehensive web-based platform for creating
sophisticated, interactive dashboards and visualizations from time
series data. It features an extensive browser-based interface that
enables users to build complex analytical dashboards without
programming knowledge, while still supporting advanced configuration
options for power users.

Overview
--------

Tshistory Dashboard provides a complete web-based solution for:

- **Interactive Dashboard Builder**: Visual, declarative dashboard composition
- **Advanced Figure Editor**: Sophisticated chart creation with real-time preview
- **Search and Discovery**: Comprehensive figure management and organization
- **Multi-trace Visualizations**: Complex plots with multiple data series
- **Hierarchical Dashboards**: Multi-level menu structures
- **Form-driven Configuration**: Smart forms that adapt to user selections
- **Plugin Architecture**: Extensible with custom figure types


Interactive Dashboard Builder
-----------------------------

Visual Dashboard Composition
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The dashboard builder provides a comprehensive visual interface for
creating hierarchical dashboards:

**Tree Structure Editor:**
- Configure multi-level dashboard menus
- Visual hierarchy management with expand/collapse controls
- Drag-and-drop reordering of dashboard sections
- Real-time validation with visual feedback

**Grid-based Layout Manager:**
- Excel-like interface for organizing dashboard elements
- Interactive table editor with dynamic row/column management
- Smart placeholders with auto-suggestion based on previous entries
- Level management with data preservation warnings

**Dashboard Configuration:**
- Metadata management (title, descriptions)
- Section organization with named groupings
- Form integration for interactive data filtering
- Graph assignment to specific dashboard positions
- Redundancy detection and conflict resolution

Creating a New Dashboard
~~~~~~~~~~~~~~~~~~~~~~~~

1. **Access the Dashboard Builder**: Navigate to the dashboard creation interface
2. **Configure Hierarchy**: Set up your dashboard's menu structure
3. **Assign Graphs**: Link existing figures to dashboard positions
4. **Add Sections**: Organize related visualizations under named sections
5. **Preview**: Review the dashboard layout before saving
6. **Save and Publish**: Make the dashboard available to users

Advanced Figure Editor
----------------------

Interactive Graph Creation
~~~~~~~~~~~~~~~~~~~~~~~~~~

The figure editor provides a powerful interface for creating complex
visualizations:

**Multi-trace Management:**
- Add unlimited data traces to single graphs
- Individual parameter configuration per trace
- Visual reordering with drag-and-drop functionality
- Bulk operations for efficient trace management

**Dynamic Parameter Forms:**
- Automatically generated forms based on graph type
- Conditional fields that appear/disappear based on selections
- Real-time validation with user feedback
- Interactive help system with examples and documentation

**Preview System:**
- Live preview with real-time graph generation
- Floating/dockable preview windows for improved workflow
- Interactive plots with full Plotly.js integration
- Error handling with clear diagnostic messages

Data Source Integration
~~~~~~~~~~~~~~~~~~~~~~~

**Catalog Browser:**
- Search and select from available time series catalogs
- Auto-complete functionality for efficient data discovery
- Context-aware filtering based on data types
- Query interface with keyword-based filtering

**Date and Time Handling:**
- Smart date parsing and validation
- Support for complex date expressions
- Business date calculations (quarters, fiscal years, etc.)
- Relative date specifications (last month, year-to-date, etc.)

Search and Discovery System
----------------------------

Advanced Figure Management
~~~~~~~~~~~~~~~~~~~~~~~~~~

The search interface provides comprehensive figure discovery
capabilities:

**Multi-criteria Search:**
- Intelligent keyword-based filtering across figure names
- Type-based organization with visual badges
- Combination filters (name + type) for precise results
- Real-time search results with instant feedback

**Figure Organization:**
- Type-based categorization (timeseries, tables, custom types)
- Direct editing access with one-click navigation
- Embedded preview functionality
- Batch management operations

Interactive Form System
-----------------------

Specification-Driven UI
~~~~~~~~~~~~~~~~~~~~~~~~

The dashboard system features intelligent forms that adapt to user
needs:

**User Experience Features:**
- Auto-completion for catalog searches
- Mandatory field highlighting with clear visual indicators
- Reset functionality for easy form clearing
- Real-time input validation with actionable error messages
- Conditional logic with fields that appear/disappear based on context
- Support for multiple input types (text, numbers, dates, selections)
- Smart defaults and placeholder text for user guidance

**Advanced Input Types:**
- Date/time selectors with business date support
- Catalog browsers with search and filtering
- Multi-select interfaces for complex configurations
- Color pickers and styling controls

Chart Types and Visualization
-----------------------------

Supported Visualizations
~~~~~~~~~~~~~~~~~~~~~~~~

**Time Series Charts:**
- Line plots with multiple styling options
- Area charts with configurable fill modes
- Seasonal overlays for year-over-year analysis
- Multi-axis support for different data scales

**Bar and Column Charts:**
- Stacked and grouped bar configurations
- Horizontal and vertical orientations
- Custom color schemes and styling options

**Data Tables:**
- Interactive tabular data display
- Sorting and filtering capabilities
- Conditional formatting rules
- Export functionality (CSV, Excel, PDF)

**Custom Visualizations:**
- Plugin support for specialized chart types
- Balance sheet tables (via tshistory_balance plugin)
- Geographic visualizations (when available)
- Extensible architecture for custom implementations

Interactive Features
~~~~~~~~~~~~~~~~~~~~

**Plot Interactivity:**
- Zoom and pan capabilities
- Rich tooltips with contextual information
- Interactive legends with series toggling
- Cross-filtering between related charts

**Dashboard Navigation:**
- Hierarchical menu systems with breadcrumb navigation
- Direct links to detailed analysis views
- Embedded figure viewing without page navigation
- Shareable URLs for specific dashboard states


Legacy CSV Configuration Support
---------------------------------

While the web interface is the primary creation method, Tshistory
Dashboard maintains support for CSV-based configuration for:

**Migration Scenarios:**
- Importing existing dashboard configurations
- Bulk creation of similar visualizations
- Automated dashboard generation via scripts
- Integration with external data processing systems

**Power User Features:**
- Complex parameter configurations
- Batch operations on multiple figures
- Version control for configuration files
- Automated testing of dashboard configurations


Best Practices
--------------

**User Experience:**
- Design for your target audience (executives, analysts, operators)
- Maintain consistent color schemes and styling
- Provide clear navigation and context
- Include relevant metadata (data sources, update times)

**Performance Considerations:**
- Limit the number of series per visualization
- Use appropriate time aggregation for analysis periods
- Monitor and optimize query performance (using materialized formulas)

**Maintenance Strategy:**
- Regular review and cleanup of unused figures
- Documentation of business logic and requirements
- Testing with realistic data volumes and user loads


This comprehensive web-based platform enables organizations to create
sophisticated analytical interfaces that scale from simple
visualizations to complex multi-dashboard systems, all through an
intuitive browser-based interface that requires no programming
expertise.
