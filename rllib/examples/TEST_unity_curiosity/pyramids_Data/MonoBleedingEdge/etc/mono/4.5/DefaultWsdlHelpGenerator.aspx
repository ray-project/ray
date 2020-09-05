<%--
//
// DefaultWsdlHelpGenerator.aspx: 
//
// Author:
//   Lluis Sanchez Gual (lluis@ximian.com)
//
// (C) 2003 Ximian, Inc.  http://www.ximian.com
//
--%>

<%@ Import Namespace="System.Collections" %>
<%@ Import Namespace="System.Collections.Generic" %>
<%@ Import Namespace="System.IO" %>
<%@ Import Namespace="System.Xml.Serialization" %>
<%@ Import Namespace="System.Xml" %>
<%@ Import Namespace="System.Xml.Schema" %>
<%@ Import Namespace="System.Web.Services" %>
<%@ Import Namespace="System.Web.Services.Description" %>
<%@ Import Namespace="System.Web.Services.Configuration" %>
<%@ Import Namespace="System.Web.Configuration" %>
<%@ Import Namespace="System" %>
<%@ Import Namespace="System.Net" %>
<%@ Import Namespace="System.Globalization" %>
<%@ Import Namespace="System.Resources" %>
<%@ Import Namespace="System.Diagnostics" %>
<%@ Import Namespace="System.CodeDom" %>
<%@ Import Namespace="System.CodeDom.Compiler" %>
<%@ Import Namespace="Microsoft.CSharp" %>
<%@ Import Namespace="Microsoft.VisualBasic" %>
<%@ Import Namespace="System.Text" %>
<%@ Import Namespace="System.Text.RegularExpressions" %>
<%@ Import Namespace="System.Security.Cryptography.X509Certificates" %>
<%@ Assembly name="System.Web.Services" %>
<%@ Page debug="true" %>

<html>
<script language="C#" runat="server">

ServiceDescriptionCollection descriptions;
XmlSchemas schemas;

string WebServiceName;
string WebServiceDescription;
string PageName;

string DefaultBinding;
ArrayList ServiceProtocols;

string CurrentOperationName;
string CurrentOperationBinding;
string OperationDocumentation;
string CurrentOperationFormat;
bool CurrentOperationSupportsTest;
ArrayList InParams;
ArrayList OutParams;
string CurrentOperationProtocols;
int CodeTextColumns = 95;
BasicProfileViolationCollection ProfileViolations;

void Page_Load(object sender, EventArgs e)
{
	descriptions = (ServiceDescriptionCollection) Context.Items["wsdls"];
	schemas = (XmlSchemas) Context.Items["schemas"];

	ServiceDescription desc = descriptions [0];
	if (schemas.Count == 0) schemas = desc.Types.Schemas;
	
	Service service = desc.Services[0];
	WebServiceName = service.Name;
	if (desc.Bindings.Count == 0)
		return;
	
	DefaultBinding = desc.Bindings[0].Name;
	WebServiceDescription = service.Documentation;
	if (WebServiceDescription == "" || WebServiceDescription == null)
		WebServiceDescription = "Description has not been provided";
	ServiceProtocols = FindServiceProtocols (null);
	
	CurrentOperationName = Request.QueryString["op"];
	CurrentOperationBinding = Request.QueryString["bnd"];
	if (CurrentOperationName != null) BuildOperationInfo ();

	PageName = HttpUtility.UrlEncode (Path.GetFileName(Request.Path), Encoding.UTF8);

	ArrayList list = new ArrayList ();
	foreach (ServiceDescription sd in descriptions) {
		foreach (Binding bin in sd.Bindings)
			if (bin.Extensions.Find (typeof(SoapBinding)) != null) list.Add (bin);
	}

	BindingsRepeater.DataSource = list;
	Page.DataBind();
	
	ProfileViolations = new BasicProfileViolationCollection ();
	foreach (WsiProfilesElement claims in ((WebServicesSection) WebConfigurationManager.GetSection("system.web/webServices")).ConformanceWarnings)
		if (claims.Name != WsiProfiles.None)
			WebServicesInteroperability.CheckConformance (claims.Name, descriptions, ProfileViolations);
}

void BuildOperationInfo ()
{
	InParams = new ArrayList ();
	OutParams = new ArrayList ();
	
	Port port = FindPort (CurrentOperationBinding, null);
	Binding binding = descriptions.GetBinding (port.Binding);
	
	PortType portType = descriptions.GetPortType (binding.Type);
	Operation oper = FindOperation (portType, CurrentOperationName);
	
	OperationDocumentation = oper.Documentation;
	if (OperationDocumentation == null || OperationDocumentation == "")
		OperationDocumentation = "No additional remarks";
	
	foreach (OperationMessage opm in oper.Messages)
	{
		if (opm is OperationInput)
			BuildParameters (InParams, opm);
		else if (opm is OperationOutput)
			BuildParameters (OutParams, opm);
	}
	
	// Protocols supported by the operation
	CurrentOperationProtocols = "";
	WebServiceProtocols testProtocols = 0;
	ArrayList prots = FindServiceProtocols (CurrentOperationName);
	for (int n=0; n<prots.Count; n++) {
		string prot = (string) prots [n];
		if (n != 0) CurrentOperationProtocols += ", ";
		CurrentOperationProtocols += prot;
		if (prot == "HttpGet")
			testProtocols |= WebServiceProtocols.HttpGet;
		else if (prot == "HttpPost") {
			testProtocols |= WebServiceProtocols.HttpPost;
			if (Context.Request.IsLocal)
				testProtocols |= WebServiceProtocols.HttpPostLocalhost;
		}
	}
	CurrentOperationSupportsTest = (WebServicesSection.Current.EnabledProtocols & testProtocols) != 0;

	// Operation format
	OperationBinding obin = FindOperation (binding, CurrentOperationName);
	if (obin != null)
		CurrentOperationFormat = GetOperationFormat (obin);

	InputParamsRepeater.DataSource = InParams;
	InputFormParamsRepeater.DataSource = InParams;
	OutputParamsRepeater.DataSource = OutParams;
}

void BuildParameters (ArrayList list, OperationMessage opm)
{
	Message msg = descriptions.GetMessage (opm.Message);
	if (msg.Parts.Count > 0 && msg.Parts[0].Name == "parameters")
	{
		MessagePart part = msg.Parts[0];
		XmlSchemaComplexType ctype;
		if (part.Element == XmlQualifiedName.Empty)
		{
			ctype = (XmlSchemaComplexType) schemas.Find (part.Type, typeof(XmlSchemaComplexType));
		}
		else
		{
			XmlSchemaElement elem = (XmlSchemaElement) schemas.Find (part.Element, typeof(XmlSchemaElement));
			ctype = (XmlSchemaComplexType) elem.SchemaType;
		}
		XmlSchemaSequence seq = ctype.Particle as XmlSchemaSequence;
		if (seq == null) return;
		
		foreach (XmlSchemaObject ob in seq.Items)
		{
			Parameter p = new Parameter();
			p.Description = "No additional remarks";
			
			if (ob is XmlSchemaElement)
			{
				XmlSchemaElement selem = GetRefElement ((XmlSchemaElement)ob);
				p.Name = selem.Name;
				p.Type = selem.SchemaTypeName.Name;
			}
			else
			{
				p.Name = "Unknown";
				p.Type = "Unknown";
			}
			list.Add (p);
		}
	}
	else
	{
		foreach (MessagePart part in msg.Parts)
		{
			Parameter p = new Parameter ();
			p.Description = "No additional remarks";
			p.Name = part.Name;
			if (part.Element == XmlQualifiedName.Empty)
				p.Type = part.Type.Name;
			else
			{
				XmlSchemaElement elem = (XmlSchemaElement) schemas.Find (part.Element, typeof(XmlSchemaElement));
				p.Type = elem.SchemaTypeName.Name;
			}
			list.Add (p);
		}
	}
}

string GetOperationFormat (OperationBinding obin)
{
	string format = "";
	SoapOperationBinding sob = obin.Extensions.Find (typeof(SoapOperationBinding)) as SoapOperationBinding;
	if (sob != null) {
		format = sob.Style.ToString ();
		SoapBodyBinding sbb = obin.Input.Extensions.Find (typeof(SoapBodyBinding)) as SoapBodyBinding;
		if (sbb != null)
			format += " / " + sbb.Use;
	}
	return format;
}

XmlSchemaElement GetRefElement (XmlSchemaElement elem)
{
	if (!elem.RefName.IsEmpty)
		return (XmlSchemaElement) schemas.Find (elem.RefName, typeof(XmlSchemaElement));
	else
		return elem;
}

ArrayList FindServiceProtocols(string operName)
{
	ArrayList table = new ArrayList ();
	Service service = descriptions[0].Services[0];
	foreach (Port port in service.Ports)
	{
		string prot = null;
		Binding bin = descriptions.GetBinding (port.Binding);
		if (bin.Extensions.Find (typeof(SoapBinding)) != null)
			prot = "Soap";
		else 
		{
			HttpBinding hb = (HttpBinding) bin.Extensions.Find (typeof(HttpBinding));
			if (hb != null && hb.Verb == "POST") prot = "HttpPost";
			else if (hb != null && hb.Verb == "GET") prot = "HttpGet";
		}
		
		if (prot != null && operName != null)
		{
			if (FindOperation (bin, operName) == null)
				prot = null;
		}

		if (prot != null && !table.Contains (prot))
			table.Add (prot);
	}
	return table;
}

Port FindPort (string portName, string protocol)
{
	Service service = descriptions[0].Services[0];
	foreach (Port port in service.Ports)
	{
		if (portName == null)
		{
			Binding binding = descriptions.GetBinding (port.Binding);
			if (GetProtocol (binding) == protocol) return port;
		}
		else if (port.Name == portName)
			return port;
	}
	return null;
}

string GetProtocol (Binding binding)
{
	if (binding.Extensions.Find (typeof(SoapBinding)) != null) return "Soap";
	HttpBinding hb = (HttpBinding) binding.Extensions.Find (typeof(HttpBinding));
	if (hb == null) return "";
	if (hb.Verb == "POST") return "HttpPost";
	if (hb.Verb == "GET") return "HttpGet";
	return "";
}


Operation FindOperation (PortType portType, string name)
{
	foreach (Operation oper in portType.Operations) {
		if (oper.Messages.Input.Name != null) {
			if (oper.Messages.Input.Name == name) return oper;
		}
		else
			if (oper.Name == name) return oper;
	}
		
	return null;
}

OperationBinding FindOperation (Binding binding, string name)
{
	foreach (OperationBinding oper in binding.Operations) {
		if (oper.Input.Name != null) {
			if (oper.Input.Name == name) return oper;
		}
		else 
			if (oper.Name == name) return oper;
	}
		
	return null;
}

string FormatBindingName (string name)
{
	if (name == DefaultBinding) return "Methods";
	else return "Methods for binding<br>" + name;
}

string GetOpName (object op)
{
	OperationBinding ob = op as OperationBinding;
	if (ob == null) return "";
	if (ob.Input.Name != null) return ob.Input.Name;
	else return ob.Name;
}

bool HasFormResult
{
	get { return Request.QueryString ["ext"] == "testform"; }
}

class NoCheckCertificatePolicy : ICertificatePolicy {
	public bool CheckValidationResult (ServicePoint a, X509Certificate b, WebRequest c, int d)
	{
		return true;
	}
}

string GetOrPost ()
{
	return (CurrentOperationProtocols.IndexOf ("HttpGet") >= 0) ? "GET" : "POST";
}

string GetQS ()
{
	bool fill = false;
	string qs = "";
	NameValueCollection query_string = Request.QueryString;
	for (int n = 0; n < query_string.Count; n++) {
		if (fill) {
			if (qs != "") qs += "&";
			qs += query_string.GetKey(n) + "=" + Server.UrlEncode (query_string [n]);
		}
		if (query_string.GetKey(n) == "ext") fill = true;
	}

	return qs;
}

string GetTestResultUrl ()
{ 
	if (!HasFormResult) return "";
	
	string location = null;
	ServiceDescription desc = descriptions [0];
	Service service = desc.Services[0];
	foreach (Port port in service.Ports)
		if (port.Name == CurrentOperationBinding)
		{
			SoapAddressBinding sbi = (SoapAddressBinding) port.Extensions.Find (typeof(SoapAddressBinding));
			if (sbi != null)
				location = sbi.Location;
		}

	if (location == null) 
		return "Could not locate web service";
	
	return location + "/" + CurrentOperationName;
}

string GenerateOperationMessages (string protocol, bool generateInput)
{
	if (!IsOperationSupported (protocol)) return "";
	
	Port port;
	if (protocol != "Soap") port = FindPort (null, protocol);
	else port = FindPort (CurrentOperationBinding, null);
	
	Binding binding = descriptions.GetBinding (port.Binding);
	OperationBinding obin = FindOperation (binding, CurrentOperationName);
	PortType portType = descriptions.GetPortType (binding.Type);
	Operation oper = FindOperation (portType, CurrentOperationName);
	
	HtmlSampleGenerator sg = new HtmlSampleGenerator (descriptions, schemas);
	string txt = sg.GenerateMessage (port, obin, oper, protocol, generateInput);
	if (protocol == "Soap") txt = WrapText (txt,CodeTextColumns);
	txt = ColorizeXml (txt);
	txt = txt.Replace ("@placeholder!","<span class='literal-placeholder'>");
	txt = txt.Replace ("!placeholder@","</span>");
	return txt;
}

bool IsOperationSupported (string protocol)
{
	if (CurrentPage != "op" || CurrentTab != "msg") return false;
	if (protocol == "Soap") return true;

	Port port = FindPort (null, protocol);
	if (port == null) return false;
	Binding binding = descriptions.GetBinding (port.Binding);
	if (binding == null) return false;
	return FindOperation (binding, CurrentOperationName) != null;
}

//
// Proxy code generation
//

string GetProxyCode ()
{
	CodeNamespace codeNamespace = new CodeNamespace();
	CodeCompileUnit codeUnit = new CodeCompileUnit();
	
	codeUnit.Namespaces.Add (codeNamespace);

	ServiceDescriptionImporter importer = new ServiceDescriptionImporter();
	
	foreach (ServiceDescription sd in descriptions)
		importer.AddServiceDescription(sd, null, null);

	foreach (XmlSchema sc in schemas)
		importer.Schemas.Add (sc);

	importer.Import(codeNamespace, codeUnit);

	string langId = Request.QueryString ["lang"];
	if (langId == null || langId == "") langId = "cs";
	CodeDomProvider provider = GetProvider (langId);
	ICodeGenerator generator = provider.CreateGenerator();
	CodeGeneratorOptions options = new CodeGeneratorOptions();
	
	StringWriter sw = new StringWriter ();
	generator.GenerateCodeFromCompileUnit(codeUnit, sw, options);

	return Colorize (WrapText (sw.ToString (), CodeTextColumns), langId);
}

public string CurrentLanguage
{
	get {
		string langId = Request.QueryString ["lang"];
		if (langId == null || langId == "") langId = "cs";
		return langId;
	}
}

public string CurrentProxytName
{
	get {
		string lan = CurrentLanguage == "cs" ? "C#" : "Visual Basic";
		return lan + " Client Proxy";
	}
}

private CodeDomProvider GetProvider(string langId)
{
	switch (langId.ToUpper())
	{
		case "CS": return new CSharpCodeProvider();
		case "VB": return new VBCodeProvider();
		default: return null;
	}
}

//
// Document generation
//
class UTF8StringWriter : StringWriter {
	public override Encoding Encoding {
		get { return Encoding.UTF8; }
	}
}

string GenerateDocument ()
{
	UTF8StringWriter sw = new UTF8StringWriter ();
	
	if (CurrentDocType == "wsdl")
		descriptions [CurrentDocInd].Write (sw);
	else if (CurrentDocType == "schema")
		schemas [CurrentDocInd].Write (sw);
		
	return Colorize (WrapText (sw.ToString (), CodeTextColumns), "xml");
}

public string CurrentDocType
{
	get { return Request.QueryString ["doctype"] != null ? Request.QueryString ["doctype"] : "wsdl"; }
}

public int CurrentDocInd
{
	get { return Request.QueryString ["docind"] != null ? int.Parse (Request.QueryString ["docind"]) : 0; }
}

public string CurrentDocumentName
{
	get {
		if (CurrentDocType == "wsdl")
			return "WSDL document for namespace \"" + descriptions [CurrentDocInd].TargetNamespace + "\"";
		else
			return "Xml Schema for namespace \"" + schemas [CurrentDocInd].TargetNamespace + "\"";
	}
}

//
// Pages and tabs
//

bool firstTab = true;
ArrayList disabledTabs = new ArrayList ();

string CurrentTab
{
	get { return Request.QueryString["tab"] != null ? Request.QueryString["tab"] : "main" ; }
}

string CurrentPage
{
	get { return Request.QueryString["page"] != null ? Request.QueryString["page"] : "main" ; }
}

void WriteTabs ()
{
	if (CurrentOperationName != null)
	{
		WriteTab ("main","Overview");
		WriteTab ("test","Test Form");
		WriteTab ("msg","Message Layout");
	}
}

void WriteTab (string id, string label)
{
	if (!firstTab) Response.Write("&nbsp;|&nbsp;");
	firstTab = false;
	
	string cname = CurrentTab == id ? "tabLabelOn" : "tabLabelOff";
	Response.Write ("<a href='" + PageName + "?" + GetPageContext(null) + GetDataContext() + "tab=" + id + "' style='text-decoration:none'>");
	Response.Write ("<span class='" + cname + "'>" + label + "</span>");
	Response.Write ("</a>");
}

string GetTabContext (string pag, string tab)
{
	if (tab == null) tab = CurrentTab;
	if (pag == null) pag = CurrentPage;
	if (pag != CurrentPage) tab = "main";
	return "page=" + pag + "&tab=" + tab + "&"; 
}

string GetPageContext (string pag)
{
	if (pag == null) pag = CurrentPage;
	return "page=" + pag + "&"; 
}

class Tab
{
	public string Id;
	public string Label;
}

//
// Syntax coloring
//

static string keywords_cs =
	"(\\babstract\\b|\\bevent\\b|\\bnew\\b|\\bstruct\\b|\\bas\\b|\\bexplicit\\b|\\bnull\\b|\\bswitch\\b|\\bbase\\b|\\bextern\\b|" +
	"\\bobject\\b|\\bthis\\b|\\bbool\\b|\\bfalse\\b|\\boperator\\b|\\bthrow\\b|\\bbreak\\b|\\bfinally\\b|\\bout\\b|\\btrue\\b|" +
	"\\bbyte\\b|\\bfixed\\b|\\boverride\\b|\\btry\\b|\\bcase\\b|\\bfloat\\b|\\bparams\\b|\\btypeof\\b|\\bcatch\\b|\\bfor\\b|" +
	"\\bprivate\\b|\\buint\\b|\\bchar\\b|\\bforeach\\b|\\bprotected\\b|\\bulong\\b|\\bchecked\\b|\\bgoto\\b|\\bpublic\\b|" +
	"\\bunchecked\\b|\\bclass\\b|\\bif\\b|\\breadonly\\b|\\bunsafe\\b|\\bconst\\b|\\bimplicit\\b|\\bref\\b|\\bushort\\b|" +
	"\\bcontinue\\b|\\bin\\b|\\breturn\\b|\\busing\\b|\\bdecimal\\b|\\bint\\b|\\bsbyte\\b|\\bvirtual\\b|\\bdefault\\b|" +
	"\\binterface\\b|\\bsealed\\b|\\bvolatile\\b|\\bdelegate\\b|\\binternal\\b|\\bshort\\b|\\bvoid\\b|\\bdo\\b|\\bis\\b|" +
	"\\bsizeof\\b|\\bwhile\\b|\\bdouble\\b|\\block\\b|\\bstackalloc\\b|\\belse\\b|\\blong\\b|\\bstatic\\b|\\benum\\b|" +
	"\\bnamespace\\b|\\bstring\\b)";

static string keywords_vb =
	"(\\bAddHandler\\b|\\bAddressOf\\b|\\bAlias\\b|\\bAnd\\b|\\bAndAlso\\b|\\bAnsi\\b|\\bAs\\b|\\bAssembly\\b|" +
	"\\bAuto\\b|\\bBoolean\\b|\\bByRef\\b|\\bByte\\b|\\bByVal\\b|\\bCall\\b|\\bCase\\b|\\bCatch\\b|" +
	"\\bCBool\\b|\\bCByte\\b|\\bCChar\\b|\\bCDate\\b|\\bCDec\\b|\\bCDbl\\b|\\bChar\\b|\\bCInt\\b|" +
	"\\bClass\\b|\\bCLng\\b|\\bCObj\\b|\\bConst\\b|\\bCShort\\b|\\bCSng\\b|\\bCStr\\b|\\bCType\\b|" +
	"\\bDate\\b|\\bDecimal\\b|\\bDeclare\\b|\\bDefault\\b|\\bDelegate\\b|\\bDim\\b|\\bDirectCast\\b|\\bDo\\b|" +
	"\\bDouble\\b|\\bEach\\b|\\bElse\\b|\\bElseIf\\b|\\bEnd\\b|\\bEnum\\b|\\bErase\\b|\\bError\\b|" +
	"\\bEvent\\b|\\bExit\\b|\\bFalse\\b|\\bFinally\\b|\\bFor\\b|\\bFriend\\b|\\bFunction\\b|\\bGet\\b|" +
	"\\bGetType\\b|\\bGoSub\\b|\\bGoTo\\b|\\bHandles\\b|\\bIf\\b|\\bImplements\\b|\\bImports\\b|\\bIn\\b|" +
	"\\bInherits\\b|\\bInteger\\b|\\bInterface\\b|\\bIs\\b|\\bLet\\b|\\bLib\\b|\\bLike\\b|\\bLong\\b|" +
	"\\bLoop\\b|\\bMe\\b|\\bMod\\b|\\bModule\\b|\\bMustInherit\\b|\\bMustOverride\\b|\\bMyBase\\b|\\bMyClass\\b|" +
	"\\bNamespace\\b|\\bNew\\b|\\bNext\\b|\\bNot\\b|\\bNothing\\b|\\bNotInheritable\\b|\\bNotOverridable\\b|\\bObject\\b|" +
	"\\bOn\\b|\\bOption\\b|\\bOptional\\b|\\bOr\\b|\\bOrElse\\b|\\bOverloads\\b|\\bOverridable\\b|\\bOverrides\\b|" +
	"\\bParamArray\\b|\\bPreserve\\b|\\bPrivate\\b|\\bProperty\\b|\\bProtected\\b|\\bPublic\\b|\\bRaiseEvent\\b|\\bReadOnly\\b|" +
	"\\bReDim\\b|\\bREM\\b|\\bRemoveHandler\\b|\\bResume\\b|\\bReturn\\b|\\bSelect\\b|\\bSet\\b|\\bShadows\\b|" +
	"\\bShared\\b|\\bShort\\b|\\bSingle\\b|\\bStatic\\b|\\bStep\\b|\\bStop\\b|\\bString\\b|\\bStructure\\b|" +
	"\\bSub\\b|\\bSyncLock\\b|\\bThen\\b|\\bThrow\\b|\\bTo\\b|\\bTrue\\b|\\bTry\\b|\\bTypeOf\\b|" +
	"\\bUnicode\\b|\\bUntil\\b|\\bVariant\\b|\\bWhen\\b|\\bWhile\\b|\\bWith\\b|\\bWithEvents\\b|\\bWriteOnly\\b|\\bXor\\b)";

string Colorize (string text, string lang)
{
	if (lang == "xml") return ColorizeXml (text);
	else if (lang == "cs") return ColorizeCs (text);
	else if (lang == "vb") return ColorizeVb (text);
	else return text;
}

string ColorizeXml (string text)
{
	text = text.Replace (" ", "&nbsp;");
	Regex re = new Regex ("\r\n|\r|\n");
	text = re.Replace (text, "_br_");
	
	re = new Regex ("<\\s*(\\/?)\\s*([\\s\\S]*?)\\s*(\\/?)\\s*>");
	text = re.Replace (text,"{blue:&lt;$1}{maroon:$2}{blue:$3&gt;}");
	
	re = new Regex ("\\{(\\w*):([\\s\\S]*?)\\}");
	text = re.Replace (text,"<span style='color:$1'>$2</span>");

	re = new Regex ("\"(.*?)\"");
	text = re.Replace (text,"\"<span style='color:purple'>$1</span>\"");

	
	text = text.Replace ("\t", "&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;");
	text = text.Replace ("_br_", "<br>");
	return text;
}

string ColorizeCs (string text)
{
	text = text.Replace (" ", "&nbsp;");

	text = text.Replace ("<", "&lt;");
	text = text.Replace (">", "&gt;");

	Regex re = new Regex ("\"((((?!\").)|\\\")*?)\"");
	text = re.Replace (text,"<span style='color:purple'>\"$1\"</span>");

	re = new Regex ("//(((.(?!\"</span>))|\"(((?!\").)*)\"</span>)*)(\r|\n|\r\n)");
	text = re.Replace (text,"<span style='color:green'>//$1</span><br/>");
	
	re = new Regex (keywords_cs);
	text = re.Replace (text,"<span style='color:blue'>$1</span>");
	
	text = text.Replace ("\t","&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;");
	text = text.Replace ("\n","<br/>");
	
	return text;
}

string ColorizeVb (string text)
{
	text = text.Replace (" ", "&nbsp;");
	
/*	Regex re = new Regex ("\"((((?!\").)|\\\")*?)\"");
	text = re.Replace (text,"<span style='color:purple'>\"$1\"</span>");

	re = new Regex ("'(((.(?!\"\\<\\/span\\>))|\"(((?!\").)*)\"\\<\\/span\\>)*)(\r|\n|\r\n)");
	text = re.Replace (text,"<span style='color:green'>//$1</span><br/>");
	
	re = new Regex (keywords_vb);
	text = re.Replace (text,"<span style='color:blue'>$1</span>");
*/	
	text = text.Replace ("\t","&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;");
	text = text.Replace ("\n","<br/>");
	return text;
}

//
// Helper methods and classes
//

string GetDataContext ()
{
	return "op=" + CurrentOperationName + "&bnd=" + CurrentOperationBinding + "&";
}

string GetOptionSel (string v1, string v2)
{
	string op = "<option ";
	if (v1 == v2) op += "selected ";
	return op + "value='" + v1 + "'>";
}

string WrapText (string text, int maxChars)
{
	text =  text.Replace(" />","/>");
	
	string linspace = null;
	int lincount = 0;
	int breakpos = 0;
	int linstart = 0;
	bool inquotes = false;
	char lastc = ' ';
	string sublineIndent = "";
	System.Text.StringBuilder sb = new System.Text.StringBuilder ();
	for (int n=0; n<text.Length; n++)
	{
		char c = text [n];
		
		if (c=='\r' || c=='\n' || n==text.Length-1)
		{
			sb.Append (linspace + sublineIndent + text.Substring (linstart, n-linstart+1));
			linspace = null;
			lincount = 0;
			linstart = n+1;
			breakpos = linstart;
			sublineIndent = "";
			lastc = c;
			continue;
		}
		
		if (lastc==',' || lastc=='(')
		{
			if (!inquotes) breakpos = n;
		}
		
		if (lincount > maxChars && breakpos >= linstart)
		{
			if (linspace != null)
				sb.Append (linspace + sublineIndent);
			sb.Append (text.Substring (linstart, breakpos-linstart));
			sb.Append ("\n");
			sublineIndent = "     ";
			lincount = linspace.Length + sublineIndent.Length + (n-breakpos);
			linstart = breakpos;
		}
		
		if (c==' ' || c=='\t')
		{
			if (!inquotes)
				breakpos = n;
		}
		else if (c=='"')
		{
			inquotes = !inquotes;
		}
		else 
			if (linspace == null) {
				linspace = text.Substring (linstart, n-linstart);
				linstart = n;
			}

		lincount++;
		lastc = c;
	}
	return sb.ToString ();
}

class Parameter
{
	string name;
	string type;
	string description;

	public string Name { get { return name; } set { name = value; } }
	public string Type { get { return type; } set { type = value; } }
	public string Description { get { return description; } set { description = value; } }
}

public class HtmlSampleGenerator: SampleGenerator
{
	public HtmlSampleGenerator (ServiceDescriptionCollection services, XmlSchemas schemas)
	: base (services, schemas)
	{
	}
		
	protected override string GetLiteral (string s)
	{
		return "@placeholder!" + s + "!placeholder@";
	}
}


	public class SampleGenerator
	{
		protected ServiceDescriptionCollection descriptions;
		protected XmlSchemas schemas;
		XmlSchemaElement anyElement;
		ArrayList queue;
		SoapBindingUse currentUse;
		XmlDocument document = new XmlDocument ();
		
		static readonly XmlQualifiedName anyType = new XmlQualifiedName ("anyType",XmlSchema.Namespace);
		static readonly XmlQualifiedName arrayType = new XmlQualifiedName ("Array","http://schemas.xmlsoap.org/soap/encoding/");
		static readonly XmlQualifiedName arrayTypeRefName = new XmlQualifiedName ("arrayType","http://schemas.xmlsoap.org/soap/encoding/");
		const string SoapEnvelopeNamespace = "http://schemas.xmlsoap.org/soap/envelope/";
		const string WsdlNamespace = "http://schemas.xmlsoap.org/wsdl/";
		const string SoapEncodingNamespace = "http://schemas.xmlsoap.org/soap/encoding/";
		
		class EncodedType
		{
			public EncodedType (string ns, XmlSchemaElement elem) { Namespace = ns; Element = elem; }
			public string Namespace;
			public XmlSchemaElement Element;
		}

		public SampleGenerator (ServiceDescriptionCollection services, XmlSchemas schemas)
		{
			descriptions = services;
			this.schemas = schemas;
			queue = new ArrayList ();
		}
		
		public string GenerateMessage (Port port, OperationBinding obin, Operation oper, string protocol, bool generateInput)
		{
			OperationMessage msg = null;
			foreach (OperationMessage opm in oper.Messages)
			{
				if (opm is OperationInput && generateInput) msg = opm;
				else if (opm is OperationOutput && !generateInput) msg = opm;
			}
			if (msg == null) return null;
			
			switch (protocol) {
				case "Soap": return GenerateHttpSoapMessage (port, obin, oper, msg);
				case "HttpGet": return GenerateHttpGetMessage (port, obin, oper, msg);
				case "HttpPost": return GenerateHttpPostMessage (port, obin, oper, msg);
			}
			return "Unknown protocol";
		}
		
		public string GenerateHttpSoapMessage (Port port, OperationBinding obin, Operation oper, OperationMessage msg)
		{
			string req = "";
			
			if (msg is OperationInput)
			{
				SoapAddressBinding sab = port.Extensions.Find (typeof(SoapAddressBinding)) as SoapAddressBinding;
				SoapOperationBinding sob = obin.Extensions.Find (typeof(SoapOperationBinding)) as SoapOperationBinding;
				req += "POST " + new Uri (sab.Location).AbsolutePath + "\n";
				req += "SOAPAction: " + sob.SoapAction + "\n";
				req += "Content-Type: text/xml; charset=utf-8\n";
				req += "Content-Length: " + GetLiteral ("string") + "\n";
				req += "Host: " + GetLiteral ("string") + "\n\n";
			}
			else
			{
				req += "HTTP/1.0 200 OK\n";
				req += "Content-Type: text/xml; charset=utf-8\n";
				req += "Content-Length: " + GetLiteral ("string") + "\n\n";
			}
			
			req += GenerateSoapMessage (obin, oper, msg);
			return req;
		}
		
		public string GenerateHttpGetMessage (Port port, OperationBinding obin, Operation oper, OperationMessage msg)
		{
			string req = "";
			
			if (msg is OperationInput)
			{
				HttpAddressBinding sab = port.Extensions.Find (typeof(HttpAddressBinding)) as HttpAddressBinding;
				HttpOperationBinding sob = obin.Extensions.Find (typeof(HttpOperationBinding)) as HttpOperationBinding;
				string location = new Uri (sab.Location).AbsolutePath + sob.Location + "?" + BuildQueryString (msg);
				req += "GET " + location + "\n";
				req += "Host: " + GetLiteral ("string");
			}
			else
			{
				req += "HTTP/1.0 200 OK\n";
				req += "Content-Type: text/xml; charset=utf-8\n";
				req += "Content-Length: " + GetLiteral ("string") + "\n\n";
			
				MimeXmlBinding mxb = (MimeXmlBinding) obin.Output.Extensions.Find (typeof(MimeXmlBinding)) as MimeXmlBinding;
				if (mxb == null) return req;
				
				Message message = descriptions.GetMessage (msg.Message);
				XmlQualifiedName ename = null;
				foreach (MessagePart part in message.Parts)
					if (part.Name == mxb.Part) ename = part.Element;
					
				if (ename == null) return req + GetLiteral("string");
				
				StringWriter sw = new StringWriter ();
				XmlTextWriter xtw = new XmlTextWriter (sw);
				xtw.Formatting = Formatting.Indented;
				currentUse = SoapBindingUse.Literal;
				WriteRootElementSample (xtw, ename);
				xtw.Close ();
				req += sw.ToString ();
			}
			
			return req;
		}
		
		public string GenerateHttpPostMessage (Port port, OperationBinding obin, Operation oper, OperationMessage msg)
		{
			string req = "";
			
			if (msg is OperationInput)
			{
				HttpAddressBinding sab = port.Extensions.Find (typeof(HttpAddressBinding)) as HttpAddressBinding;
				HttpOperationBinding sob = obin.Extensions.Find (typeof(HttpOperationBinding)) as HttpOperationBinding;
				string location = new Uri (sab.Location).AbsolutePath + sob.Location;
				req += "POST " + location + "\n";
				req += "Content-Type: application/x-www-form-urlencoded\n";
				req += "Content-Length: " + GetLiteral ("string") + "\n";
				req += "Host: " + GetLiteral ("string") + "\n\n";
				req += BuildQueryString (msg);
			}
			else return GenerateHttpGetMessage (port, obin, oper, msg);
			
			return req;
		}
		
		string BuildQueryString (OperationMessage opm)
		{
			string s = "";
			Message msg = descriptions.GetMessage (opm.Message);
			foreach (MessagePart part in msg.Parts)
			{
				if (s.Length != 0) s += "&";
				s += part.Name + "=" + GetLiteral (part.Type.Name);
			}
			return s;
		}
		
		public string GenerateSoapMessage (OperationBinding obin, Operation oper, OperationMessage msg)
		{
			SoapOperationBinding sob = obin.Extensions.Find (typeof(SoapOperationBinding)) as SoapOperationBinding;
			SoapBindingStyle style = (sob != null) ? sob.Style : SoapBindingStyle.Document;
			
			MessageBinding msgbin = (msg is OperationInput) ? (MessageBinding) obin.Input : (MessageBinding)obin.Output;
			SoapBodyBinding sbb = msgbin.Extensions.Find (typeof(SoapBodyBinding)) as SoapBodyBinding;
			SoapBindingUse bodyUse = (sbb != null) ? sbb.Use : SoapBindingUse.Literal;
			
			StringWriter sw = new StringWriter ();
			XmlTextWriter xtw = new XmlTextWriter (sw);
			xtw.Formatting = Formatting.Indented;
			
			xtw.WriteStartDocument ();
			xtw.WriteStartElement ("soap", "Envelope", SoapEnvelopeNamespace);
			xtw.WriteAttributeString ("xmlns", "xsi", null, XmlSchema.InstanceNamespace);
			xtw.WriteAttributeString ("xmlns", "xsd", null, XmlSchema.Namespace);
			
			if (bodyUse == SoapBindingUse.Encoded) 
			{
				xtw.WriteAttributeString ("xmlns", "soapenc", null, SoapEncodingNamespace);
				xtw.WriteAttributeString ("xmlns", "tns", null, msg.Message.Namespace);
			}

			// Serialize headers
			
			bool writtenHeader = false;
			foreach (object ob in msgbin.Extensions)
			{
				SoapHeaderBinding hb = ob as SoapHeaderBinding;
				if (hb == null) continue;
				
				if (!writtenHeader) {
					xtw.WriteStartElement ("soap", "Header", SoapEnvelopeNamespace);
					writtenHeader = true;
				}
				
				WriteHeader (xtw, hb);
			}
			
			if (writtenHeader)
				xtw.WriteEndElement ();

			// Serialize body
			xtw.WriteStartElement ("soap", "Body", SoapEnvelopeNamespace);
			
			currentUse = bodyUse;
			WriteBody (xtw, oper, msg, sbb, style);
			
			xtw.WriteEndElement ();
			xtw.WriteEndElement ();
			xtw.Close ();
			return sw.ToString ();
		}
		
		void WriteHeader (XmlTextWriter xtw, SoapHeaderBinding header)
		{
			Message msg = descriptions.GetMessage (header.Message);
			if (msg == null) throw new InvalidOperationException ("Message " + header.Message + " not found");
			MessagePart part = msg.Parts [header.Part];
			if (part == null) throw new InvalidOperationException ("Message part " + header.Part + " not found in message " + header.Message);

			currentUse = header.Use;
			
			if (currentUse == SoapBindingUse.Literal)
				WriteRootElementSample (xtw, part.Element);
			else
				WriteTypeSample (xtw, part.Type);
		}
		
		void WriteBody (XmlTextWriter xtw, Operation oper, OperationMessage opm, SoapBodyBinding sbb, SoapBindingStyle style)
		{
			Message msg = descriptions.GetMessage (opm.Message);
			if (msg.Parts.Count > 0 && msg.Parts[0].Name == "parameters")
			{
				MessagePart part = msg.Parts[0];
				if (part.Element == XmlQualifiedName.Empty)
					WriteTypeSample (xtw, part.Type);
				else
					WriteRootElementSample (xtw, part.Element);
			}
			else
			{
				string elemName = oper.Name;
				string ns = "";
				if (opm is OperationOutput) elemName += "Response";
				
				if (style == SoapBindingStyle.Rpc) {
					xtw.WriteStartElement (elemName, sbb.Namespace);
					ns = sbb.Namespace;
				}
					
				foreach (MessagePart part in msg.Parts)
				{
					if (part.Element == XmlQualifiedName.Empty)
					{
						XmlSchemaElement elem = new XmlSchemaElement ();
						elem.SchemaTypeName = part.Type;
						elem.Name = part.Name;
						WriteElementSample (xtw, ns, elem);
					}
					else
						WriteRootElementSample (xtw, part.Element);
				}
				
				if (style == SoapBindingStyle.Rpc)
					xtw.WriteEndElement ();
			}
			WriteQueuedTypeSamples (xtw);
		}
		
		void WriteRootElementSample (XmlTextWriter xtw, XmlQualifiedName qname)
		{
			XmlSchemaElement elem = (XmlSchemaElement) schemas.Find (qname, typeof(XmlSchemaElement));
			if (elem == null) throw new InvalidOperationException ("Element not found: " + qname);
			WriteElementSample (xtw, qname.Namespace, elem);
		}

		void WriteElementSample (XmlTextWriter xtw, string ns, XmlSchemaElement elem)
		{
			bool sharedAnnType = false;
			XmlQualifiedName root;
			
			if (!elem.RefName.IsEmpty) {
				XmlSchemaElement refElem = FindRefElement (elem);
				if (refElem == null) throw new InvalidOperationException ("Global element not found: " + elem.RefName);
				root = elem.RefName;
				elem = refElem;
				sharedAnnType = true;
			}
			else
				root = new XmlQualifiedName (elem.Name, ns);
			
			if (!elem.SchemaTypeName.IsEmpty)
			{
				XmlSchemaComplexType st = FindComplexTyype (elem.SchemaTypeName);
				if (st != null) 
					WriteComplexTypeSample (xtw, st, root);
				else
				{
					xtw.WriteStartElement (root.Name, root.Namespace);
					if (currentUse == SoapBindingUse.Encoded) 
						xtw.WriteAttributeString ("type", XmlSchema.InstanceNamespace, GetQualifiedNameString (xtw, elem.SchemaTypeName));
					xtw.WriteString (GetLiteral (FindBuiltInType (elem.SchemaTypeName)));
					xtw.WriteEndElement ();
				}
			}
			else if (elem.SchemaType == null)
			{
				xtw.WriteStartElement ("any");
				xtw.WriteEndElement ();
			}
			else
				WriteComplexTypeSample (xtw, (XmlSchemaComplexType) elem.SchemaType, root);
		}
		
		void WriteTypeSample (XmlTextWriter xtw, XmlQualifiedName qname)
		{
			XmlSchemaComplexType ctype = FindComplexTyype (qname);
			if (ctype != null) {
				WriteComplexTypeSample (xtw, ctype, qname);
				return;
			}
			
			XmlSchemaSimpleType stype = (XmlSchemaSimpleType) schemas.Find (qname, typeof(XmlSchemaSimpleType));
			if (stype != null) {
				WriteSimpleTypeSample (xtw, stype);
				return;
			}
			
			xtw.WriteString (GetLiteral (FindBuiltInType (qname)));
			throw new InvalidOperationException ("Type not found: " + qname);
		}
		
		void WriteComplexTypeSample (XmlTextWriter xtw, XmlSchemaComplexType stype, XmlQualifiedName rootName)
		{
			WriteComplexTypeSample (xtw, stype, rootName, -1);
		}
		
		void WriteComplexTypeSample (XmlTextWriter xtw, XmlSchemaComplexType stype, XmlQualifiedName rootName, int id)
		{
			string ns = rootName.Namespace;
			
			if (rootName.Name.IndexOf ("[]") != -1) rootName = arrayType;
			
			if (currentUse == SoapBindingUse.Encoded) {
				string pref = xtw.LookupPrefix (rootName.Namespace);
				if (pref == null) pref = "q1";
				xtw.WriteStartElement (pref, rootName.Name, rootName.Namespace);
				ns = "";
			}
			else
				xtw.WriteStartElement (rootName.Name, rootName.Namespace);
			
			if (id != -1)
			{
				xtw.WriteAttributeString ("id", "id" + id);
				if (rootName != arrayType)
					xtw.WriteAttributeString ("type", XmlSchema.InstanceNamespace, GetQualifiedNameString (xtw, rootName));
			}
			
			WriteComplexTypeAttributes (xtw, stype);
			WriteComplexTypeElements (xtw, ns, stype);
			
			xtw.WriteEndElement ();
		}
		
		void WriteComplexTypeAttributes (XmlTextWriter xtw, XmlSchemaComplexType stype)
		{
			WriteAttributes (xtw, stype.Attributes, stype.AnyAttribute);
		}

		Dictionary<XmlSchemaComplexType,int> recursed_types = new Dictionary<XmlSchemaComplexType,int> ();
		void WriteComplexTypeElements (XmlTextWriter xtw, string ns, XmlSchemaComplexType stype)
		{
			int prev = 0;
			if (recursed_types.ContainsKey (stype))
				prev = recursed_types [stype];

			if (prev > 1)
				return;
			recursed_types [stype] = ++prev;

			if (stype.Particle != null)
				WriteParticleComplexContent (xtw, ns, stype.Particle);
			else
			{
				if (stype.ContentModel is XmlSchemaSimpleContent)
					WriteSimpleContent (xtw, (XmlSchemaSimpleContent)stype.ContentModel);
				else if (stype.ContentModel is XmlSchemaComplexContent)
					WriteComplexContent (xtw, ns, (XmlSchemaComplexContent)stype.ContentModel);
			}
			prev = recursed_types [stype];
			recursed_types [stype] = --prev;
		}

		void WriteAttributes (XmlTextWriter xtw, XmlSchemaObjectCollection atts, XmlSchemaAnyAttribute anyat)
		{
			foreach (XmlSchemaObject at in atts)
			{
				if (at is XmlSchemaAttribute)
				{
					string ns;
					XmlSchemaAttribute attr = (XmlSchemaAttribute)at;
					XmlSchemaAttribute refAttr = attr;
					
					// refAttr.Form; TODO
					
					if (!attr.RefName.IsEmpty) {
						refAttr = FindRefAttribute (attr.RefName);
						if (refAttr == null) throw new InvalidOperationException ("Global attribute not found: " + attr.RefName);
					}
					
					string val;
					if (!refAttr.SchemaTypeName.IsEmpty) val = FindBuiltInType (refAttr.SchemaTypeName);
					else val = FindBuiltInType ((XmlSchemaSimpleType) refAttr.SchemaType);
					
					xtw.WriteAttributeString (refAttr.Name, val);
				}
				else if (at is XmlSchemaAttributeGroupRef)
				{
					XmlSchemaAttributeGroupRef gref = (XmlSchemaAttributeGroupRef)at;
					XmlSchemaAttributeGroup grp = (XmlSchemaAttributeGroup) schemas.Find (gref.RefName, typeof(XmlSchemaAttributeGroup));
					WriteAttributes (xtw, grp.Attributes, grp.AnyAttribute);
				}
			}
			
			if (anyat != null)
				xtw.WriteAttributeString ("custom-attribute","value");
		}
		
		void WriteParticleComplexContent (XmlTextWriter xtw, string ns, XmlSchemaParticle particle)
		{
			WriteParticleContent (xtw, ns, particle, false);
		}
		
		void WriteParticleContent (XmlTextWriter xtw, string ns, XmlSchemaParticle particle, bool multiValue)
		{
			if (particle is XmlSchemaGroupRef)
				particle = GetRefGroupParticle ((XmlSchemaGroupRef)particle);

			if (particle.MaxOccurs > 1) multiValue = true;
			
			if (particle is XmlSchemaSequence) {
				WriteSequenceContent (xtw, ns, ((XmlSchemaSequence)particle).Items, multiValue);
			}
			else if (particle is XmlSchemaChoice) {
				if (((XmlSchemaChoice)particle).Items.Count == 1)
					WriteSequenceContent (xtw, ns, ((XmlSchemaChoice)particle).Items, multiValue);
				else
					WriteChoiceContent (xtw, ns, (XmlSchemaChoice)particle, multiValue);
			}
			else if (particle is XmlSchemaAll) {
				WriteSequenceContent (xtw, ns, ((XmlSchemaAll)particle).Items, multiValue);
			}
		}

		void WriteSequenceContent (XmlTextWriter xtw, string ns, XmlSchemaObjectCollection items, bool multiValue)
		{
			foreach (XmlSchemaObject item in items)
				WriteContentItem (xtw, ns, item, multiValue);
		}
		
		void WriteContentItem (XmlTextWriter xtw, string ns, XmlSchemaObject item, bool multiValue)
		{
			if (item is XmlSchemaGroupRef)
				item = GetRefGroupParticle ((XmlSchemaGroupRef)item);
					
			if (item is XmlSchemaElement)
			{
				XmlSchemaElement elem = (XmlSchemaElement) item;
				XmlSchemaElement refElem;
				if (!elem.RefName.IsEmpty) refElem = FindRefElement (elem);
				else refElem = elem;

				int num = (elem.MaxOccurs == 1 && !multiValue) ? 1 : 2;
				for (int n=0; n<num; n++)
				{
					if (currentUse == SoapBindingUse.Literal)
						WriteElementSample (xtw, ns, refElem);
					else
						WriteRefTypeSample (xtw, ns, refElem);
				}
			}
			else if (item is XmlSchemaAny)
			{
				xtw.WriteString (GetLiteral ("xml"));
			}
			else if (item is XmlSchemaParticle) {
				WriteParticleContent (xtw, ns, (XmlSchemaParticle)item, multiValue);
			}
		}
		
		void WriteChoiceContent (XmlTextWriter xtw, string ns, XmlSchemaChoice choice, bool multiValue)
		{
			foreach (XmlSchemaObject item in choice.Items)
				WriteContentItem (xtw, ns, item, multiValue);
		}

		void WriteSimpleContent (XmlTextWriter xtw, XmlSchemaSimpleContent content)
		{
			XmlSchemaSimpleContentExtension ext = content.Content as XmlSchemaSimpleContentExtension;
			if (ext != null)
				WriteAttributes (xtw, ext.Attributes, ext.AnyAttribute);
				
			XmlQualifiedName qname = GetContentBaseType (content.Content);
			xtw.WriteString (GetLiteral (FindBuiltInType (qname)));
		}

		string FindBuiltInType (XmlQualifiedName qname)
		{
			if (qname.Namespace == XmlSchema.Namespace)
				return qname.Name;

			XmlSchemaComplexType ct = FindComplexTyype (qname);
			if (ct != null)
			{
				XmlSchemaSimpleContent sc = ct.ContentModel as XmlSchemaSimpleContent;
				if (sc == null) throw new InvalidOperationException ("Invalid schema");
				return FindBuiltInType (GetContentBaseType (sc.Content));
			}
			
			XmlSchemaSimpleType st = (XmlSchemaSimpleType) schemas.Find (qname, typeof(XmlSchemaSimpleType));
			if (st != null)
				return FindBuiltInType (st);

			throw new InvalidOperationException ("Definition of type " + qname + " not found");
		}

		string FindBuiltInType (XmlSchemaSimpleType st)
		{
			if (st.Content is XmlSchemaSimpleTypeRestriction) {
				return FindBuiltInType (GetContentBaseType (st.Content));
			}
			else if (st.Content is XmlSchemaSimpleTypeList) {
				string s = FindBuiltInType (GetContentBaseType (st.Content));
				return s + " " + s + " ...";
			}
			else if (st.Content is XmlSchemaSimpleTypeUnion)
			{
				//Check if all types of the union are equal. If not, then will use anyType.
				XmlSchemaSimpleTypeUnion uni = (XmlSchemaSimpleTypeUnion) st.Content;
				string utype = null;

				// Anonymous types are unique
				if (uni.BaseTypes.Count != 0 && uni.MemberTypes.Length != 0)
					return "string";

				foreach (XmlQualifiedName mt in uni.MemberTypes)
				{
					string qn = FindBuiltInType (mt);
					if (utype != null && qn != utype) return "string";
					else utype = qn;
				}
				return utype;
			}
			else
				return "string";
		}
		

		XmlQualifiedName GetContentBaseType (XmlSchemaObject ob)
		{
			if (ob is XmlSchemaSimpleContentExtension)
				return ((XmlSchemaSimpleContentExtension)ob).BaseTypeName;
			else if (ob is XmlSchemaSimpleContentRestriction)
				return ((XmlSchemaSimpleContentRestriction)ob).BaseTypeName;
			else if (ob is XmlSchemaSimpleTypeRestriction)
				return ((XmlSchemaSimpleTypeRestriction)ob).BaseTypeName;
			else if (ob is XmlSchemaSimpleTypeList)
				return ((XmlSchemaSimpleTypeList)ob).ItemTypeName;
			else
				return null;
		}

		void WriteComplexContent (XmlTextWriter xtw, string ns, XmlSchemaComplexContent content)
		{
			XmlQualifiedName qname;

			XmlSchemaComplexContentExtension ext = content.Content as XmlSchemaComplexContentExtension;
			if (ext != null) qname = ext.BaseTypeName;
			else {
				XmlSchemaComplexContentRestriction rest = (XmlSchemaComplexContentRestriction)content.Content;
				qname = rest.BaseTypeName;
				if (qname == arrayType) {
					ParseArrayType (rest, out qname);
					XmlSchemaElement elem = new XmlSchemaElement ();
					elem.Name = "Item";
					elem.SchemaTypeName = qname;
					
					xtw.WriteAttributeString ("arrayType", SoapEncodingNamespace, qname.Name + "[2]");
					WriteContentItem (xtw, ns, elem, true);
					return;
				}
			}
			
			// Add base map members to this map
			XmlSchemaComplexType ctype = FindComplexTyype (qname);
			WriteComplexTypeAttributes (xtw, ctype);
			
			if (ext != null) {
				// Add the members of this map
				WriteAttributes (xtw, ext.Attributes, ext.AnyAttribute);
				if (ext.Particle != null)
					WriteParticleComplexContent (xtw, ns, ext.Particle);
			}
			
			WriteComplexTypeElements (xtw, ns, ctype);
		}
		
		void ParseArrayType (XmlSchemaComplexContentRestriction rest, out XmlQualifiedName qtype)
		{
			XmlSchemaAttribute arrayTypeAt = FindArrayAttribute (rest.Attributes);
			XmlAttribute[] uatts = arrayTypeAt.UnhandledAttributes;
			if (uatts == null || uatts.Length == 0) throw new InvalidOperationException ("arrayType attribute not specified in array declaration");
			
			XmlAttribute xat = null;
			foreach (XmlAttribute at in uatts)
				if (at.LocalName == "arrayType" && at.NamespaceURI == WsdlNamespace)
					{ xat = at; break; }
			
			if (xat == null) 
				throw new InvalidOperationException ("arrayType attribute not specified in array declaration");
			
			string arrayType = xat.Value;
			string type, ns;
			int i = arrayType.LastIndexOf (":");
			if (i == -1) ns = "";
			else ns = arrayType.Substring (0,i);
			
			int j = arrayType.IndexOf ("[", i+1);
			if (j == -1) throw new InvalidOperationException ("Cannot parse WSDL array type: " + arrayType);
			type = arrayType.Substring (i+1);
			type = type.Substring (0, type.Length-2);
			
			qtype = new XmlQualifiedName (type, ns);
		}
		
		XmlSchemaAttribute FindArrayAttribute (XmlSchemaObjectCollection atts)
		{
			foreach (object ob in atts)
			{
				XmlSchemaAttribute att = ob as XmlSchemaAttribute;
				if (att != null && att.RefName == arrayTypeRefName) return att;
				
				XmlSchemaAttributeGroupRef gref = ob as XmlSchemaAttributeGroupRef;
				if (gref != null)
				{
					XmlSchemaAttributeGroup grp = (XmlSchemaAttributeGroup) schemas.Find (gref.RefName, typeof(XmlSchemaAttributeGroup));
					att = FindArrayAttribute (grp.Attributes);
					if (att != null) return att;
				}
			}
			return null;
		}
		
		void WriteSimpleTypeSample (XmlTextWriter xtw, XmlSchemaSimpleType stype)
		{
			xtw.WriteString (GetLiteral (FindBuiltInType (stype)));
		}
		
		XmlSchemaParticle GetRefGroupParticle (XmlSchemaGroupRef refGroup)
		{
			XmlSchemaGroup grp = (XmlSchemaGroup) schemas.Find (refGroup.RefName, typeof (XmlSchemaGroup));
			return grp.Particle;
		}

		XmlSchemaElement FindRefElement (XmlSchemaElement elem)
		{
			if (elem.RefName.Namespace == XmlSchema.Namespace)
			{
				if (anyElement != null) return anyElement;
				anyElement = new XmlSchemaElement ();
				anyElement.Name = "any";
				anyElement.SchemaTypeName = anyType;
				return anyElement;
			}
			return (XmlSchemaElement) schemas.Find (elem.RefName, typeof(XmlSchemaElement));
		}
		
		XmlSchemaAttribute FindRefAttribute (XmlQualifiedName refName)
		{
			if (refName.Namespace == XmlSchema.Namespace)
			{
				XmlSchemaAttribute at = new XmlSchemaAttribute ();
				at.Name = refName.Name;
				at.SchemaTypeName = new XmlQualifiedName ("string",XmlSchema.Namespace);
				return at;
			}
			return (XmlSchemaAttribute) schemas.Find (refName, typeof(XmlSchemaAttribute));
		}
		
		void WriteRefTypeSample (XmlTextWriter xtw, string ns, XmlSchemaElement elem)
		{
			if (elem.SchemaTypeName.Namespace == XmlSchema.Namespace || schemas.Find (elem.SchemaTypeName, typeof(XmlSchemaSimpleType)) != null)
				WriteElementSample (xtw, ns, elem);
			else
			{
				xtw.WriteStartElement (elem.Name, ns);
				xtw.WriteAttributeString ("href", "#id" + (queue.Count+1));
				xtw.WriteEndElement ();
				queue.Add (new EncodedType (ns, elem));
			}
		}
		
		void WriteQueuedTypeSamples (XmlTextWriter xtw)
		{
			for (int n=0; n<queue.Count; n++)
			{
				EncodedType ec = (EncodedType) queue[n];
				XmlSchemaComplexType st = FindComplexTyype (ec.Element.SchemaTypeName);
				WriteComplexTypeSample (xtw, st, ec.Element.SchemaTypeName, n+1);
			}
		}
		
		XmlSchemaComplexType FindComplexTyype (XmlQualifiedName qname)
		{
			if (qname.Name.IndexOf ("[]") != -1)
			{
				XmlSchemaComplexType stype = new XmlSchemaComplexType ();
				stype.ContentModel = new XmlSchemaComplexContent ();
				
				XmlSchemaComplexContentRestriction res = new XmlSchemaComplexContentRestriction ();
				stype.ContentModel.Content = res;
				res.BaseTypeName = arrayType;
				
				XmlSchemaAttribute att = new XmlSchemaAttribute ();
				att.RefName = arrayTypeRefName;
				res.Attributes.Add (att);
				
				XmlAttribute xat = document.CreateAttribute ("arrayType", WsdlNamespace);
				xat.Value = qname.Namespace + ":" + qname.Name;
				att.UnhandledAttributes = new XmlAttribute[] {xat};
				return stype;
			}
				
			return (XmlSchemaComplexType) schemas.Find (qname, typeof(XmlSchemaComplexType));
		}
		
		string GetQualifiedNameString (XmlTextWriter xtw, XmlQualifiedName qname)
		{
			string pref = xtw.LookupPrefix (qname.Namespace);
			if (pref != null) return pref + ":" + qname.Name;
			
			xtw.WriteAttributeString ("xmlns", "q1", null, qname.Namespace);
			return "q1:" + qname.Name;
		}
				
		protected virtual string GetLiteral (string s)
		{
			return s;
		}

		void GetOperationFormat (OperationBinding obin, out SoapBindingStyle style, out SoapBindingUse use)
		{
			style = SoapBindingStyle.Document;
			use = SoapBindingUse.Literal;
			SoapOperationBinding sob = obin.Extensions.Find (typeof(SoapOperationBinding)) as SoapOperationBinding;
			if (sob != null) {
				style = sob.Style;
				SoapBodyBinding sbb = obin.Input.Extensions.Find (typeof(SoapBodyBinding)) as SoapBodyBinding;
				if (sbb != null)
					use = sbb.Use;
			}
		}
	}





</script>

<head runat="server">
	<%
	Response.Write ("<link rel=\"alternate\" type=\"text/xml\" href=\"" + Request.FilePath + "?disco\"/>");
	%>
	<title><%=WebServiceName%> Web Service</title>
    <style type="text/css">
		BODY { font-family: Arial; margin-left: 20px; margin-top: 20px; font-size: x-small}
		TABLE { font-size: x-small }
		.title { color:dimgray; font-family: Arial; font-size:20pt; font-weight:900}
		.operationTitle { color:dimgray; font-family: Arial; font-size:15pt; font-weight:900}
		.method { font-size: x-small }
		.bindingLabel { font-size: x-small; font-weight:bold; color:darkgray; line-height:8pt; display:block; margin-bottom:3px }
		.label { font-size: small; font-weight:bold; color:darkgray }
		.paramTable { font-size: x-small }
		.paramTable TR { background-color: gainsboro }
		.paramFormTable { font-size: x-small; padding: 10px; background-color: gainsboro }
		.paramFormTable TR { background-color: gainsboro }
		.paramInput { border: solid 1px gray }
		.button {border: solid 1px gray }
		.smallSeparator { height:3px; overflow:hidden }
		.panel { background-color:whitesmoke; border: solid 1px silver; border-top: solid 1px silver  }
		.codePanel { background-color: white; font-size:x-small; padding:7px; border:solid 1px silver}
		.code-xml { font-size:10pt; font-family:courier }
		.code-cs { font-size:10pt; font-family:courier }
		.code-vb { font-size:10pt; font-family:courier }
		.tabLabelOn { font-weight:bold }
		.tabLabelOff {color: darkgray }
		.literal-placeholder {color: darkblue; font-weight:bold}
		A:link { color: black; }
		A:visited { color: black; }
		A:active { color: black; }
		A:hover { color: blue }
    </style>
	
<script language="javascript" type="text/javascript">
var req;
function getXML (command, url, qs) {
	if (url == "" || url.substring (0, 4) != "http")
		return;
	
	var post_data = null;
	req = getReq ();
	req.onreadystatechange = stateChange;
	if (command == "GET") {
		url = url + "?" + qs;
	} else {
		post_data = qs;
	}
	req.open (command, url,  true); 
	if (command == "POST")
		req.setRequestHeader ("Content-Type", "application/x-www-form-urlencoded");
	req.send (post_data); 
}

function stateChange () {
	if (req.readyState == 4) {
		var node = document.getElementById("testresult_div");
		var text = "";
		if (req.status == 200) {
			node.innerHTML = "<div class='code-xml'>" + formatXml (req.responseText) + "</div>";
		} else {
			var ht = "<b style='color: red'>" + formatXml (req.status + " - " + req.statusText) + "</b>";
			if (req.responseText != "")
				ht = ht + "\n<div class='code-xml'>" + formatXml (req.responseText) + "</div>";
			node.innerHTML = ht;
					
		}
	}
}

function formatXml (text)
{	
	var re = / /g;
	text = text.replace (re, "&nbsp;");

	re = /\t/g;
	text = text.replace (re, "&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;");
	
	re = /\<\s*(\/?)\s*(.*?)\s*(\/?)\s*\>/g;
	text = text.replace (re,"{blue:&lt;$1}{maroon:$2}{blue:$3&gt;}");
	
	re = /{(\w*):(.*?)}/g;
	text = text.replace (re,"<span style='color:$1'>$2</span>");

	re = /"(.*?)"/g;
	text = text.replace (re,"\"<span style='color:purple'>$1</span>\"");

	re = /\r\n|\r|\n/g;
	text = text.replace (re, "<br/>");
	
	return text;
}

function getReq () {
	if (window.XMLHttpRequest) {
		return new XMLHttpRequest();     // Firefox, Safari, ...
	} else if (window.ActiveXObject) {
		return new ActiveXObject("Microsoft.XMLHTTP");
	}
}

function clearForm ()
{
	document.getElementById("testFormResult").style.display="none";
}
</script>

</head>

<body>
<div class="title" style="margin-left:20px">
<span class="label">Web Service</span><br>
<%=WebServiceName%>
</div>

<!--
	**********************************************************
	Left panel
-->

<table border="0" width="100%" cellpadding="15px" cellspacing="15px">
<tr valign="top"><td width="150px" class="panel">
<div style="width:150px"></div>
<a class="method" href='<%=PageName%>'>Overview</a><br>
<div class="smallSeparator"></div>
<a class="method" href='<%=PageName + "?" + GetPageContext("wsdl")%>'>Service Description</a>
<div class="smallSeparator"></div>
<a class="method" href='<%=PageName + "?" + GetPageContext("proxy")%>'>Client proxy</a>
<br><br>
	<asp:repeater id="BindingsRepeater" runat=server>
		<itemtemplate name="itemtemplate">
			<span class="bindingLabel"><%#FormatBindingName(DataBinder.Eval(Container.DataItem, "Name").ToString())%></span>
			<asp:repeater id="OperationsRepeater" runat=server datasource='<%# ((Binding)Container.DataItem).Operations %>'>
				<itemtemplate>
					<a class="method" href="<%=PageName%>?<%=GetTabContext("op",null)%>op=<%#GetOpName(Container.DataItem)%>&bnd=<%#DataBinder.Eval(Container.DataItem, "Binding.Name")%>"><%#GetOpName(Container.DataItem)%></a>
					<div class="smallSeparator"></div>
				</itemtemplate>
			</asp:repeater>
			<br>
		</itemtemplate>
	</asp:repeater>

</td><td class="panel">

<% if (CurrentPage == "main") {%>

<!--
	**********************************************************
	Web service overview
-->

	<p class="label">Web Service Overview</p>
	<%=WebServiceDescription%>
	<br/><br/>
	<% if (ProfileViolations != null && ProfileViolations.Count > 0) { %>
		<p class="label">Basic Profile Conformance</p>
		This web service does not conform to WS-I Basic Profile v1.1
	<%
		Response.Write ("<ul>");
		foreach (BasicProfileViolation vio in ProfileViolations) {
			Response.Write ("<li><b>" + vio.NormativeStatement + "</b>: " + vio.Details);
			Response.Write ("<ul>");
			foreach (string ele in vio.Elements)
				Response.Write ("<li>" + ele + "</li>");
			Response.Write ("</ul>");
			Response.Write ("</li>");
		}
		Response.Write ("</ul>");
	}%>

<%} if (DefaultBinding == null) {%>
This service does not contain any public web method.
<%} else if (CurrentPage == "op") {%>

<!--
	**********************************************************
	Operation description
-->

	<span class="operationTitle"><%=CurrentOperationName%></span>
	<br><br>
	<% WriteTabs (); %>
	<br><br><br>
	
	<% if (CurrentTab == "main") { %>
		<span class="label">Input Parameters</span>
		<div class="smallSeparator"></div>
		<% if (InParams.Count == 0) { %>
			No input parameters<br>
		<% } else { %>
			<table class="paramTable" cellspacing="1" cellpadding="5">
			<asp:repeater id="InputParamsRepeater" runat=server>
				<itemtemplate>
					<tr>
					<td width="150"><%#DataBinder.Eval(Container.DataItem, "Name")%></td>
					<td width="150"><%#DataBinder.Eval(Container.DataItem, "Type")%></td>
					</tr>
				</itemtemplate>
			</asp:repeater>
			</table>
		<% } %>
		<br>
		
		<% if (OutParams.Count > 0) { %>
		<span class="label">Output Parameters</span>
			<div class="smallSeparator"></div>
			<table class="paramTable" cellspacing="1" cellpadding="5">
			<asp:repeater id="OutputParamsRepeater" runat=server>
				<itemtemplate>
					<tr>
					<td width="150"><%#DataBinder.Eval(Container.DataItem, "Name")%></td>
					<td width="150"><%#DataBinder.Eval(Container.DataItem, "Type")%></td>
					</tr>
				</itemtemplate>
			</asp:repeater>
			</table>
		<br>
		<% } %>
		
		<span class="label">Remarks</span>
		<div class="smallSeparator"></div>
		<%=OperationDocumentation%>
		<br><br>
		<span class="label">Technical information</span>
		<div class="smallSeparator"></div>
		Format: <%=CurrentOperationFormat%>
		<br>Supported protocols: <%=CurrentOperationProtocols%>
	<% } %>
	
<!--
	**********************************************************
	Operation description - Test form
-->

	<% if (CurrentTab == "test") { 
		if (CurrentOperationSupportsTest) {%>
			Enter values for the parameters and click the 'Invoke' button to test this method:<br><br>
			<form action="<%=PageName%>" method="GET">
			<input type="hidden" name="page" value="<%=CurrentPage%>">
			<input type="hidden" name="tab" value="<%=CurrentTab%>">
			<input type="hidden" name="op" value="<%=CurrentOperationName%>">
			<input type="hidden" name="bnd" value="<%=CurrentOperationBinding%>">
			<input type="hidden" name="ext" value="testform">
			<table class="paramFormTable" cellspacing="0" cellpadding="3">
			<asp:repeater id="InputFormParamsRepeater" runat=server>
				<itemtemplate>
					<tr>
					<td><%#DataBinder.Eval(Container.DataItem, "Name")%>:&nbsp;</td>
					<td width="150"><input class="paramInput" type="text" size="20" name="<%#DataBinder.Eval(Container.DataItem, "Name")%>"></td>
					</tr>
				</itemtemplate>
			</asp:repeater>
			<tr><td></td><td><input class="button" type="submit" value="Invoke">&nbsp;<input class="button" type="button" onclick="clearForm()" value="Clear"></td></tr>
			</table>
			</form>
			<div id="testFormResult" style="display:<%= (HasFormResult?"block":"none") %>">
			The web service returned the following result:<br/><br/>
			<div class="codePanel" id="testresult_div">
			</div>
			<script language="javascript">
				getXML ("<%= GetOrPost () %>", "<%= GetTestResultUrl () %>", "<%= GetQS () %>");
			</script>
			</div>
		<% } else {%>
		The test form is not available for this operation because it has parameters with a complex structure.
		<% } %>
	<% } %>
	
<!--
	**********************************************************
	Operation description - Message Layout
-->

	<% if (CurrentTab == "msg") { %>
		
		The following are sample SOAP requests and responses for each protocol supported by this method:
			<br/><br/>
		
		<% if (IsOperationSupported ("Soap")) { %>
			<span class="label">Soap</span>
			<br/><br/>
			<div class="codePanel"><div class="code-xml"><%=GenerateOperationMessages ("Soap", true)%></div></div>
			<br/>
			<div class="codePanel"><div class="code-xml"><%=GenerateOperationMessages ("Soap", false)%></div></div>
			<br/>
		<% } %>
		<% if (IsOperationSupported ("HttpGet")) { %>
			<span class="label">HTTP Get</span>
			<br/><br/>
			<div class="codePanel"><div class="code-xml"><%=GenerateOperationMessages ("HttpGet", true)%></div></div>
			<br/>
			<div class="codePanel"><div class="code-xml"><%=GenerateOperationMessages ("HttpGet", false)%></div></div>
			<br/>
		<% } %>
		<% if (IsOperationSupported ("HttpPost")) { %>
			<span class="label">HTTP Post</span>
			<br/><br/>
			<div class="codePanel"><div class="code-xml"><%=GenerateOperationMessages ("HttpPost", true)%></div></div>
			<br/>
			<div class="codePanel"><div class="code-xml"><%=GenerateOperationMessages ("HttpPost", false)%></div></div>
			<br/>
		<% } %>
		
	<% } %>
<%} else if (CurrentPage == "proxy") {%>
<!--
	**********************************************************
	Client Proxy
-->
	<form action="<%=PageName%>" name="langForm" method="GET">
		Select the language for which you want to generate a proxy 
		<input type="hidden" name="page" value="<%=CurrentPage%>">&nbsp;
		<SELECT name="lang" onchange="langForm.submit()">
			<%=GetOptionSel("cs",CurrentLanguage)%>C#</option>
			<%=GetOptionSel("vb",CurrentLanguage)%>Visual Basic</option>
		</SELECT>
		&nbsp;&nbsp;
	</form>
	<br>
	<span class="label"><%=CurrentProxytName%></span>&nbsp;&nbsp;&nbsp;
	<a href="<%=PageName + "?code=" + CurrentLanguage%>">Download</a>
	<br><br>
	<div class="codePanel">
	<div class="code-<%=CurrentLanguage%>"><%=GetProxyCode ()%></div>
	</div>
<%} else if (CurrentPage == "wsdl") {%>
<!--
	**********************************************************
	Service description
-->
	<% if (descriptions.Count > 1 || schemas.Count > 1) {%>
	The description of this web service is composed by several documents. Click on the document you want to see:
	
	<ul>
	<% 
		for (int n=0; n<descriptions.Count; n++)
			Response.Write ("<li><a href='" + PageName + "?" + GetPageContext(null) + "doctype=wsdl&docind=" + n + "'>WSDL document " + descriptions[n].TargetNamespace + "</a></li>");
		for (int n=0; n<schemas.Count; n++)
			Response.Write ("<li><a href='" + PageName + "?" + GetPageContext(null) + "doctype=schema&docind=" + n + "'>Xml Schema " + schemas[n].TargetNamespace + "</a></li>");
	%>
	</ul>
	
	<%} else {%>
	<%}%>
	<br>
	<span class="label"><%=CurrentDocumentName%></span>&nbsp;&nbsp;&nbsp;
	<a href="<%=PageName + "?" + CurrentDocType + "=" + CurrentDocInd %>">Download</a>
	<br><br>
	<div class="codePanel">
	<div class="code-xml"><%=GenerateDocument ()%></div>
	</div>

<%}%>

<br><br><br><br><br><br><br><br><br><br><br><br><br><br><br>
</td>
<td width="20px"></td>
</tr>

</table>
</body>
</html>
