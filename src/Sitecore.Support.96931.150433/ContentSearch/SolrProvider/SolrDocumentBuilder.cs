namespace Sitecore.Support.ContentSearch.SolrProvider
{
  using System;
  using System.Collections.Generic;
  using System.Linq;
  using System.Text;
  using System.Threading.Tasks;
  using Sitecore.ContentSearch;
  using Sitecore.ContentSearch.Abstractions;
  using Sitecore.ContentSearch.Boosting;
  using Sitecore.ContentSearch.ComputedFields;
  using Sitecore.ContentSearch.Diagnostics;
  using Sitecore.Diagnostics;
  using System.Collections;
  using System.Collections.Concurrent;
  using System.Globalization;
  using Sitecore.ContentSearch.SolrProvider;
  using Data.LanguageFallback;

  public class SolrDocumentBuilder : AbstractDocumentBuilder<ConcurrentDictionary<string, object>>
  {
    private readonly IProviderUpdateContext Context;
    private readonly CultureInfo culture;
    private readonly SolrFieldNameTranslator fieldNameTranslator;
    private readonly ISettings settings;

    public SolrDocumentBuilder(IIndexable indexable, IProviderUpdateContext context) : base(indexable, context)
    {
      this.Context = context;
      this.fieldNameTranslator = context.Index.FieldNameTranslator as SolrFieldNameTranslator;
      this.culture = indexable.Culture;
      this.settings = context.Index.Locator.GetInstance<ISettings>();
    }

    public override void AddBoost()
    {
      float num = BoostingManager.ResolveItemBoosting(base.Indexable);
      if (num > 0f)
      {
        base.Document.GetOrAdd("_documentBoost", num);
      }
    }

    private void AddComputedIndexField(IComputedIndexField computedIndexField, ParallelLoopState parallelLoopState = null, ConcurrentQueue<Exception> exceptions = null)
    {
      object obj2;
      Assert.ArgumentNotNull(computedIndexField, "computedIndexField");
      try
      {
        obj2 = computedIndexField.ComputeFieldValue(base.Indexable);
      }
      catch (Exception exception)
      {
        CrawlingLog.Log.Warn($"Could not compute value for ComputedIndexField: {computedIndexField.FieldName} for indexable: {base.Indexable.UniqueId}", exception);
        if (base.Settings.StopOnCrawlFieldError())
        {
          if (parallelLoopState == null)
          {
            throw;
          }
          parallelLoopState.Stop();
          exceptions.Enqueue(exception);
        }
        return;
      }

      if (!string.IsNullOrEmpty(computedIndexField.ReturnType) && !base.Index.Schema.AllFieldNames.Contains(computedIndexField.FieldName))
      {
        this.AddField(computedIndexField.FieldName, obj2, computedIndexField.ReturnType);
      }
      else
      {
        this.AddField(computedIndexField.FieldName, obj2, true);
      }
    }

    public override void AddComputedIndexFields()
    {
      if (base.IsParallelComputedFieldsProcessing)
      {
        this.AddComputedIndexFieldsInParallel();
      }
      else
      {
        this.AddComputedIndexFieldsInSequence();
      }
    }

    protected virtual void AddComputedIndexFieldsInParallel()
    {
      ConcurrentQueue<Exception> exceptions = new ConcurrentQueue<Exception>();
      this.ParallelForeachProxy.ForEach<IComputedIndexField>((IEnumerable<IComputedIndexField>)base.Options.ComputedIndexFields, base.ParallelOptions, 
        (Action<IComputedIndexField, ParallelLoopState>)((field, parallelLoopState) =>
      {
        using (new LanguageFallbackFieldSwitcher(this.Index.EnableFieldLanguageFallback))
        {
          this.AddComputedIndexField(field, parallelLoopState, exceptions);
        }
      }));
    
      if (!exceptions.IsEmpty)
      {
        throw new AggregateException(exceptions);
      }
    }

    protected virtual void AddComputedIndexFieldsInSequence()
    {
      foreach (IComputedIndexField field in base.Options.ComputedIndexFields)
      {
        using (new LanguageFallbackFieldSwitcher(this.Index.EnableFieldLanguageFallback))
        {
          this.AddComputedIndexField(field, null, null);
        }
      }
    }

    public override void AddField(IIndexableDataField field)
    {
      string name = field.Name;
      object fieldValue = base.Index.Configuration.FieldReaders.GetFieldValue(field);
      AbstractSearchFieldConfiguration fieldConfiguration = this.Context.Index.Configuration.FieldMap.GetFieldConfiguration(name);
      if (((fieldConfiguration != null) && (fieldValue == null)) && ((fieldConfiguration as SolrSearchFieldConfiguration).NullValue != null))
      {
        fieldValue = (fieldConfiguration as SolrSearchFieldConfiguration).NullValue;
      }
      if (((fieldValue is string) && (fieldConfiguration != null)) && ((((string)fieldValue) == string.Empty) && ((fieldConfiguration as SolrSearchFieldConfiguration).EmptyString != null)))
      {
        fieldValue = (fieldConfiguration as SolrSearchFieldConfiguration).EmptyString;
      }
      if (((fieldValue != null) && (fieldConfiguration != null)) && (fieldValue.ToString() == string.Empty))
      {
        VerboseLogging.CrawlingLogDebug(() => $"Skipping field id:{field.Id}, name:{field.Name}, typeKey:{field.TypeKey} - Value is empty.");
        fieldValue = null;
      }
      if (fieldValue == null)
      {
        VerboseLogging.CrawlingLogDebug(() => $"Skipping field id:{field.Id}, name:{field.Name}, typeKey:{field.TypeKey} - Value is null.");
      }
      else if (string.IsNullOrEmpty(fieldValue.ToString()))
      {
        VerboseLogging.CrawlingLogDebug(() => $"Skipping field id:{field.Id}, name:{field.Name}, typeKey:{field.TypeKey} - Value is empty.");
      }
      else
      {
        float num2 = BoostingManager.ResolveFieldBoosting(field) + this.GetFieldConfigurationBoost(name);
        string fieldName = this.fieldNameTranslator.GetIndexFieldName(name, fieldValue.GetType(), this.culture);
        if (!base.IsMedia && IndexOperationsHelper.IsTextField(field))
        {
          this.StoreField(Sitecore.Search.BuiltinFields.Content, Sitecore.Search.BuiltinFields.Content, fieldValue, true, null, field.TypeKey);
        }
        this.StoreField(name, fieldName, fieldValue, false, new float?(num2), field.TypeKey);
      }
    }

    public override void AddField(string fieldName, object fieldValue, bool append = false)
    {
      AbstractSearchFieldConfiguration fieldConfiguration = this.Context.Index.Configuration.FieldMap.GetFieldConfiguration(fieldName);
      if (fieldConfiguration != null)
      {
        fieldValue = fieldConfiguration.FormatForWriting(fieldValue);
      }
      if (fieldValue == null)
      {
        VerboseLogging.CrawlingLogDebug(() => $"Skipping field name:{fieldName} - Value is empty.");
      }
      else
      {
        if (fieldValue != null && string.IsNullOrEmpty(fieldValue.ToString()))
        {
          VerboseLogging.CrawlingLogDebug(() => $"Skipping field name:{fieldName} - Value is empty.");
        }
        else
        {
          float fieldConfigurationBoost = this.GetFieldConfigurationBoost(fieldName);
          string str = this.fieldNameTranslator.GetIndexFieldName(fieldName, fieldValue.GetType(), this.culture);
          this.StoreField(fieldName, str, fieldValue, append, new float?(fieldConfigurationBoost), null);
        }
      }
    }

    private void AddField(string fieldName, object fieldValue, string returnType)
    {
      AbstractSearchFieldConfiguration fieldConfiguration = this.Context.Index.Configuration.FieldMap.GetFieldConfiguration(fieldName);
      if (((fieldConfiguration != null) && (fieldValue == null)) && ((fieldConfiguration as SolrSearchFieldConfiguration).NullValue != null))
      {
        fieldValue = (fieldConfiguration as SolrSearchFieldConfiguration).NullValue;
      }
      if (((fieldValue is string) && (fieldConfiguration != null)) && ((fieldValue.ToString() == string.Empty) && ((fieldConfiguration as SolrSearchFieldConfiguration).EmptyString != null)))
      {
        fieldValue = (fieldConfiguration as SolrSearchFieldConfiguration).EmptyString;
      }
      if (((fieldValue != null) && (fieldConfiguration != null)) && (fieldValue.ToString() == string.Empty))
      {
        VerboseLogging.CrawlingLogDebug(() => $"Skipping field name:{fieldName}, returnType:{returnType} - Value is null.");
        fieldValue = null;
      }
      if (fieldValue == null)
      {
        VerboseLogging.CrawlingLogDebug(() => $"Skipping field name:{fieldName}, returnType:{returnType} - Value is null.");
      }
      else
      {
        float fieldConfigurationBoost = this.GetFieldConfigurationBoost(fieldName);
        string str = this.fieldNameTranslator.GetIndexFieldName(fieldName, returnType, this.culture);
        this.StoreField(fieldName, str, fieldValue, false, new float?(fieldConfigurationBoost), returnType);
      }
    }

    private float GetFieldConfigurationBoost(string fieldName)
    {
      SolrSearchFieldConfiguration fieldConfiguration = this.Context.Index.Configuration.FieldMap.GetFieldConfiguration(fieldName) as SolrSearchFieldConfiguration;
      if (fieldConfiguration != null)
      {
        return fieldConfiguration.Boost;
      }
      return 0f;
    }

    private void StoreField(string unTranslatedFieldName, string fieldName, object fieldValue, bool append = false, float? boost = new float?(), string returnType = null)
    {
      object obj2 = fieldValue;
      if (base.Index.Configuration.IndexFieldStorageValueFormatter != null)
      {
        fieldValue = base.Index.Configuration.IndexFieldStorageValueFormatter.FormatValueForIndexStorage(fieldValue, unTranslatedFieldName);
      }
      if (VerboseLogging.Enabled)
      {
        StringBuilder builder1 = new StringBuilder();
        builder1.AppendFormat("Field: {0}" + Environment.NewLine, fieldName);
        builder1.AppendFormat(" - value: {0}{1}" + Environment.NewLine, (obj2 != null) ? obj2.GetType().ToString() : "NULL", (!(obj2 is string) && (obj2 is IEnumerable)) ? (" - count : " + ((IEnumerable)obj2).Cast<object>().Count<object>()) : "");
        builder1.AppendFormat(" - unformatted value: {0}" + Environment.NewLine, obj2 ?? "NULL");
        builder1.AppendFormat(" - formatted value:   {0}" + Environment.NewLine, fieldValue ?? "NULL");
        builder1.AppendFormat(" - returnType: {0}" + Environment.NewLine, returnType);
        builder1.AppendFormat(" - boost: {0}" + Environment.NewLine, boost);
        builder1.AppendFormat(" - append: {0}" + Environment.NewLine, append);
        VerboseLogging.CrawlingLogDebug(new Func<string>(builder1.ToString));
      }
      if ((append && base.Document.ContainsKey(fieldName)) && (fieldValue is string))
      {
        ConcurrentDictionary<string, object> document = base.Document;
        string str = fieldName;
        document[str] = document[str] + " " + ((string)fieldValue);
      }
      if (!base.Document.ContainsKey(fieldName))
      {
        if (boost.HasValue)
        {
          float? nullable = boost;
          float num = 0f;
          if ((nullable.GetValueOrDefault() > num) ? nullable.HasValue : false)
          {
            fieldValue = new SolrBoostedField(fieldValue, boost);
          }
        }
        base.Document.GetOrAdd(fieldName, fieldValue);
        if (this.fieldNameTranslator.HasCulture(fieldName) && !this.settings.DefaultLanguage().StartsWith(this.culture.TwoLetterISOLanguageName))
        {
          base.Document.GetOrAdd(this.fieldNameTranslator.StripKnownCultures(fieldName), fieldValue);
        }
      }
    }
  }
}