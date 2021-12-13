<?xml version="1.0"?>
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="1.0">
    <xsl:output method="text"/>
    <xsl:strip-space elements="*"/>
    <xsl:template match="error">
    <xsl:text>
</xsl:text>, <xsl:value-of select="@line"/>, <xsl:value-of
            select="@column"/>, <xsl:value-of select="@severity"/>, <xsl:value-of select="@message"/>,
        <xsl:value-of select="@source"/>
        <xsl:value-of select="../@name"/>
    </xsl:template>
</xsl:stylesheet>